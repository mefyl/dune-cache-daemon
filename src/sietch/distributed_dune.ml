open Base
open Stdune
open Utils
module Sexp = Sexplib.Sexp

module Uri = struct
  include Uri

  let compare a b = Ordering.of_int @@ compare a b

  let to_dyn uri = Dyn.String (Uri.to_string uri)
end

module Clients = Map.Make (Uri)

type t =
  { cache : Local.t
  ; mutable clients : client Clients.t
  ; config : Config.t
  }

and client =
  { requests :
      (Cohttp.Request.t * Cohttp_async.Body.t) Async_kernel.Pipe.Writer.t
  ; responses :
      (Cohttp.Response.t * Cohttp_async.Body.t) Async_kernel.Pipe.Reader.t
  ; uri : Uri.t
  }

let make cache config = { cache; clients = Clients.empty; config }

let debug = Dune_util.Log.info

let connect t uri =
  let () =
    debug [ Pp.textf "connect to distributed backend %S" (Uri.to_string uri) ]
  in
  let host = Uri.with_uri ~path:None uri in
  let requests_reader, requests = Async.Pipe.create () in
  let* responses =
    Cohttp_async.Client.callv host requests_reader
    |> Async.Deferred.map ~f:Result.return
  in
  let client = { requests; responses; uri } in
  let* clients =
    Clients.add t.clients uri client
    |> Result.map_error ~f:(fun _ -> "duplicate client")
    (* FIXME: race condition *)
    |> Async.return
  in
  let () = t.clients <- clients in
  Async.Deferred.Result.return client

let digest_of_str str =
  match Digest.from_hex str with
  | Some hash -> Async.Deferred.Result.return hash
  | None -> Async.Deferred.Result.failf "invalid hash: %S" str

let client t target =
  let find_target target =
    let f node = Config.ranges_include node.Config.space target in
    match List.find t.config.nodes ~f with
    | Some t -> Result.return t.hostname
    | None ->
      Result.Error
        (Printf.sprintf "no target backend for address %s"
           (Digest.to_string target))
  in
  let* uri = find_target target |> Async.return in
  match Clients.find t.clients uri with
  | Some client -> Async.Deferred.Result.return client
  | None -> connect t uri

let mutex = ref (Async.Ivar.create_full (Result.Ok ()))

let call t target meth ?(headers = []) ?body path =
  let* client = client t target in
  let uri =
    Uri.with_uri ~path:(Option.some @@ Uri.path client.uri ^ path) client.uri
  and headers, body, encoding =
    match body with
    | Some body ->
      ( Cohttp.Header.of_list
        @@ (("Content-Type", "application/octet-stream") :: headers)
      , body
      , Cohttp.Transfer.Chunked )
    | None ->
      ( Cohttp.Header.of_list @@ (("Content-Length", "0") :: headers)
      , Cohttp_async.Body.empty
      , Cohttp.Transfer.Fixed 0L )
  in
  let lock = !mutex
  and unlock = Async.Ivar.create () in
  let () = mutex := unlock in
  let* () =
    let request = Cohttp.Request.make ~meth ~encoding ~headers uri in
    Async.Pipe.write client.requests (request, body)
    |> Async.Deferred.map ~f:Result.return
  in
  let* () = Async.Ivar.read lock in
  let ( >>= ) = Async.( >>= ) in
  Async.Pipe.read client.responses >>= function
  | `Eof ->
    let () =
      Caml.Format.eprintf "FATAL EMPTY %s\n%!" (Digest.to_string target)
    in
    let () = Async.Ivar.fill unlock (Result.Ok ()) in
    Async.Deferred.Result.fail "error during HTTP request: no response"
  | `Ok (response, body) -> (
    (let drain () =
       Cohttp_async.Body.drain body |> Async.Deferred.map ~f:Result.return
     in
     let () = Async.Ivar.fill unlock (Result.Ok ()) in
     let* () =
       let headers = Cohttp.Response.headers response in
       match Cohttp.Header.get headers "X-dune-hash" with
       | Some hash ->
         let* hash = digest_of_str hash in
         if Poly.( = ) (Digest.compare hash target) Ordering.Eq then
           Async.Deferred.Result.return ()
         else
           let* () = drain () in
           Async.Deferred.Result.failf
             "invalid hash for artifact: expected %s, effective %s"
             (Digest.to_string target) (Digest.to_string hash)
       | None -> Async.Deferred.Result.return ()
     in
     let* body =
       Cohttp_async.Body.to_string body |> Async.Deferred.map ~f:Result.return
     in
     Async.Deferred.Result.return (Cohttp.Response.status response, body))
    >>= function
    | Result.Ok _ as res -> Async.return res
    | Result.Error _e as error -> Async.return error )

let expect_status expected m path = function
  | effective when List.exists ~f:(Poly.( = ) effective) expected ->
    Result.Ok ()
  | effective ->
    Result.Error
      (Fmt.str "unexpected %s on %s %s"
         (Cohttp.Code.string_of_status effective)
         (Cohttp.Code.string_of_method m)
         path)

let put_contents t target path contents =
  let body = Cohttp_async.Body.of_string contents in
  let* status, _body = call t target `PUT ~body path in
  Async.return @@ expect_status [ `Created; `OK ] `PUT path status

let get_file t target path local_path =
  if Path.exists local_path then
    debug
      [ Pp.textf "metadata file already present locally: %s"
          (Path.to_string local_path)
      ]
    |> Async.Deferred.Result.return
  else
    let* status, body = call t target `GET path in
    let* () = expect_status [ `OK ] `GET path status |> Async.return in
    write_file t.cache local_path true body

let distribute ({ cache; _ } as t) key (metadata : Cache.Local.Metadata_file.t)
    =
  let f () =
    let* () =
      match metadata.contents with
      | Files files ->
        let insert_file { Cache.File.digest; _ } =
          let path = Local.file_path cache digest in
          let query ?(headers = []) ?body meth =
            let path = "blocks/" ^ Digest.to_string digest in
            let* status, _body = call t digest meth ~headers ?body path in
            Async.Deferred.Result.return status
          in
          let insert stats input =
            let body = Cohttp_async.Body.of_pipe @@ Async.Reader.pipe input in
            query
              ~headers:[ ("Content-Length", Int.to_string stats.Unix.st_size) ]
              `PUT ~body
          in
          let stats = Path.stat path in
          let* upload =
            if stats.st_size < 4096 then
              Async.Deferred.Result.return true
            else
              let* status = query `HEAD in
              let* () =
                Async.return
                @@ expect_status [ `OK; `No_content ] `HEAD
                     (Path.to_string path) status
              in
              Async.Deferred.Result.return (Poly.( = ) status `No_content)
          in
          if upload then
            let* status =
              let path = Path.to_string path in
              let () = debug [ Pp.textf "distribute %S" path ] in
              Async.Reader.with_file path ~f:(insert stats)
            in
            Async.return
            @@ expect_status [ `Created; `OK ] `PUT (Path.to_string path) status
          else
            Async.Deferred.Result.return ()
        in
        let ( let* ) = Async.Deferred.( >>= ) in
        let* results =
          Async.Deferred.List.map ~how:(`Max_concurrent_jobs 1) ~f:insert_file
            files
          (* Async.Deferred.List.all @@ List.map ~f:insert_file files *)
        in
        let ( let* ) = Async.Deferred.Result.( >>= ) in
        let* (_ : unit list) = results |> Result.List.all |> Async.return in
        Async.Deferred.Result.return ()
      | Value _ -> Async.Deferred.Result.fail "ignoring Jenga value"
    in
    put_contents t key
      ("blocks/" ^ Digest.to_string key)
      (Cache.Local.Metadata_file.to_string metadata)
  in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "distribute fatal error" [ ("exception", Exn.to_dyn e) ]

let prefetch ({ cache; _ } as t) key =
  let f () =
    let local_path = Local.metadata_path cache key in
    if Path.exists local_path then
      debug
        [ Pp.textf "metadata file already present locally: %s"
            (Path.to_string local_path)
        ]
      |> Async.Deferred.Result.return
    else
      let hash = Digest.to_string key in
      let path = "blocks/" ^ hash in
      let () = debug [ Pp.textf "fetch metadata %s" hash ] in
      let* status, body = call t key `GET path in
      let* () =
        expect_status [ `OK; `No_content ] `GET path status |> Async.return
      in
      if Poly.( = ) status `No_content then
        Async.Deferred.Result.return ()
      else
        let* metadata =
          Cache.Local.Metadata_file.of_string body |> Async.return
        in
        match metadata.contents with
        | Files files ->
          let fetch { Cache.File.digest; _ } =
            let () =
              debug [ Pp.textf "fetch artifact %s" (Digest.to_string digest) ]
            in
            get_file t digest
              ("blocks/" ^ Digest.to_string digest)
              (Local.file_path cache digest)
          in
          let ( let* ) = Async.Deferred.( >>= ) in
          let* results = Async.Deferred.List.all @@ List.map ~f:fetch files in
          let ( let* ) = Async.Deferred.Result.( >>= ) in
          let* (_ : unit list) = results |> Result.List.all |> Async.return in
          write_file cache local_path false body
        | Value h ->
          let () =
            debug [ Pp.textf "skipping Jenga value: %s" (Digest.to_string h) ]
          in
          Async.Deferred.Result.return ()
  in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "prefetch fatal error" [ ("exception", Exn.to_dyn e) ]

let index_path name key =
  String.concat ~sep:"/" [ "index"; name; Digest.to_string key ]

let index_add t name key keys =
  let f () =
    put_contents t key (index_path name key)
      (String.concat ~sep:"\n" (List.map ~f:Digest.to_string keys))
  in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "index_add fatal error" [ ("exception", Exn.to_dyn e) ]

let index_prefetch t name key =
  let f () =
    let path = index_path name key in
    let* status, body = call t key `GET path in
    let* () =
      expect_status [ `OK; `Not_found ] `GET path status |> Async.return
    in
    if Poly.( = ) status `OK then
      let keys =
        String.split ~on:'\n' body
        |> List.filter_map ~f:(fun d -> Digest.from_hex d)
      in
      let ( let* ) = Async.Deferred.( >>= ) in
      let* results =
        Async.Deferred.List.map ~how:(`Max_concurrent_jobs 1) ~f:(prefetch t)
          keys
      in
      let ( let* ) = Async.Deferred.Result.( >>= ) in
      let* (_ : unit list) = Result.List.all results |> Async.return in
      Async.Deferred.Result.return ()
    else
      Async.Deferred.Result.return ()
  in
  let ( >>= ) = Async.Deferred.( >>= ) in
  Async.try_with ~extract_exn:true f >>= function
  | Result.Ok v -> Async.return v
  | Result.Error e ->
    Async.Deferred.Result.fail
      (Fmt.str "index_prefetch fatal error: %a" Exn.pp e)

let make config local =
  ( module struct
    let v =
      let local = Local.make local in
      make local config

    let distribute = distribute v

    let prefetch = prefetch v

    let index_add = index_add v

    let index_prefetch = index_prefetch v
  end : Distributed.S )
