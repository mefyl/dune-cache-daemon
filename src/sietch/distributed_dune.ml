open Stdune
open Utils
module Sexp = Sexplib.Sexp

type t =
  { cache : Local.t
  ; config : Config.t
  }

let make cache config = { cache; config }

let debug = Dune_util.Log.info

let find_target t target =
  let f node = Config.ranges_include node.Config.space target in
  match List.find t.config.nodes ~f with
  | Some t -> Result.return t.hostname
  | None ->
    Result.Error
      (Printf.sprintf "no target backend for address %s"
         (Digest.to_string target))

let call t target m ?body path =
  let ( >>= ) = Async.( >>= )
  and ( >>| ) = Async.( >>| ) in
  let* uri = find_target t target |> Async.return in
  let uri = Uri.with_uri ~path:(Option.some @@ Uri.path uri ^ path) uri
  and headers =
    Cohttp.Header.of_list [ ("Content-Type", "application/octet-stream") ]
  in
  let* response, body =
    let f () = Cohttp_async.Client.call m ~headers ?body uri in
    Async.try_with f >>= function
    | Result.Ok v -> Async.Deferred.Result.return v
    | Result.Error (Unix.Unix_error (e, f, a)) ->
      Async.Deferred.Result.fail
        (Printf.sprintf "error during HTTP request %s: %s %s"
           (Unix.error_message e) f a)
    | Result.Error e ->
      Async.Deferred.Result.fail
        (Printf.sprintf "unexpected error during HTTP request %s"
           (Printexc.to_string e))
  in
  let* body = Cohttp_async.Body.to_string body >>| Result.return in
  Async.Deferred.Result.return (Cohttp.Response.status response, body)

let expect_status expected m path = function
  | effective when List.exists ~f:(( = ) effective) expected -> Result.Ok ()
  | effective ->
    Result.Error
      (Format.sprintf "unexpected %s on %s %s"
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
    |> Lwt_result.return
  else
    let* status, body = call t target `GET path in
    let* () = expect_status [ `OK ] `GET path status |> Async.return in
    write_file t.cache local_path true body

let distribute ({ cache; _ } as t) key (metadata : Cache.Local.Metadata_file.t)
    =
  let f =
    let* () =
      match metadata.contents with
      | Files files ->
        let insert_file { Cache.File.digest; _ } =
          let path = Local.file_path cache digest in
          let query meth body =
            let path = "blocks/" ^ Digest.to_string digest in
            let* status, _body = call t digest meth ~body path in
            Lwt_result.return status
          in
          let insert input =
            let body =
              let read () =
                let%lwt res = Lwt_io.read ~count:512 input in
                if res <> "" then
                  Some res |> Lwt.return
                else
                  None |> Lwt.return
              in
              Lwt_stream.from read |> Cohttp_async.Body.of_stream
            in
            query `PUT body
          in
          let stats = Path.stat path in
          let* upload =
            if stats.st_size < 4096 then
              Lwt_result.return true
            else
              let* status = query `HEAD (Cohttp_async.Body.of_string "") in
              let* () =
                Lwt.return
                @@ expect_status [ `OK; `No_content ] `HEAD
                     (Path.to_string path) status
              in
              Lwt_result.return (status = `No_content)
          in
          if upload then
            let* status =
              Lwt_io.with_file ~mode:Lwt_io.input (Path.to_string path) insert
            in
            Lwt.return
            @@ expect_status [ `Created; `OK ] `PUT (Path.to_string path) status
          else
            Lwt_result.return ()
        in
        let%lwt results = Lwt.all @@ List.map ~f:insert_file files in
        let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
        Lwt_result.return ()
      | Value _ -> Lwt_result.fail "ignoring Jenga value"
    in
    put_contents t key
      ("blocks/" ^ Digest.to_string key)
      (Cache.Local.Metadata_file.to_string metadata)
  in
  try%lwt f
  with e -> failwith ("distribute fatal error: " ^ Printexc.to_string e)

let prefetch ({ cache; _ } as t) key =
  try%lwt
    let local_path = Local.metadata_path cache key in
    if Path.exists local_path then
      debug
        [ Pp.textf "metadata file already present locally: %s"
            (Path.to_string local_path)
        ]
      |> Lwt_result.return
    else
      let path = "blocks/" ^ Digest.to_string key in
      let* status, body = call t key `GET path in
      let* () =
        expect_status [ `OK; `No_content ] `GET path status |> Lwt.return
      in
      if status = `No_content then
        Lwt_result.return ()
      else
        let* metadata =
          Cache.Local.Metadata_file.of_string body |> Lwt.return
        in
        match metadata.contents with
        | Files files ->
          let fetch { Cache.File.digest; _ } =
            get_file t digest
              ("blocks/" ^ Digest.to_string digest)
              (Local.file_path cache digest)
          in
          let%lwt results = Lwt.all @@ List.map ~f:fetch files in
          let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
          write_file cache local_path false body
        | Value h ->
          let () =
            debug [ Pp.textf "skipping Jenga value: %s" (Digest.to_string h) ]
          in
          Lwt_result.return ()
  with e -> failwith ("prefetch fatal error: " ^ Printexc.to_string e)

let index_path name key =
  String.concat ~sep:"/" [ "index"; name; Digest.to_string key ]

let index_add t name key keys =
  try%lwt
    put_contents t key (index_path name key)
      (String.concat ~sep:"\n" (List.map ~f:Digest.to_string keys))
  with e -> failwith ("index_add fatal error: " ^ Printexc.to_string e)

let index_prefetch t name key =
  try%lwt
    let path = index_path name key in
    let* status, body = call t key `GET path in
    let* () =
      expect_status [ `OK; `Not_found ] `GET path status |> Lwt.return
    in
    if status = `OK then
      let keys =
        String.split ~on:'\n' body
        |> List.map ~f:(fun d -> Digest.from_hex d |> Option.value_exn)
      in
      let%lwt results = Lwt_list.map_s (prefetch t) keys in
      let* (_ : unit list) = Result.List.all results |> Lwt.return in
      Lwt_result.return ()
    else
      Lwt_result.return ()
  with e ->
    Lwt_result.fail ("index_prefetch fatal error: " ^ Printexc.to_string e)

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
