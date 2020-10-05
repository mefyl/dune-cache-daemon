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
  { connection : H2.Client_connection.t
  ; socket : ([ `Active ], Async.Socket.Address.Inet.t) Async.Socket.t
  ; uri : Uri.t
  }

let make cache config = { cache; clients = Clients.empty; config }

let debug = Dune_util.Log.info

let connect t uri =
  let () =
    debug [ Pp.textf "connect to distributed backend %S" (Uri.to_string uri) ]
  in
  let host = Option.value_exn (Uri.host uri)
  and port =
    match Uri.port uri with
    | Some port -> port
    | None -> (
      match Uri.scheme uri with
      | Some "https" -> 443
      | Some "http" -> 80
      | _ -> failwith "unable to determine port" )
  in
  let socket = Async.Socket.create Async.Socket.Type.tcp in
  let* client =
    let* addr_info =
      Async.Unix.Inet_addr.of_string_or_getbyname host
      |> Async.Deferred.map ~f:Result.return
    in
    let+ socket =
      Async.Socket.connect socket (`Inet (addr_info, port))
      |> Async.Deferred.map ~f:Result.return
    in
    let () = debug [ Pp.textf "CONNECTED" ] in
    let connection =
      (* FIXME: error_handler *)
      H2.Client_connection.create ?config:None ?push_handler:None
        ~error_handler:(fun _ -> ())
    in
    { connection; socket; uri }
  in
  let* clients =
    Clients.add t.clients uri client
    |> Result.map_error ~f:(fun _ -> "duplicate client")
    (* FIXME: race condition *)
    |> Async.return
  in
  let () = t.clients <- clients in
  let () =
    let ( let* ) = Async.( >>= ) in
    let io =
      let read_connection =
        let handle_chunk buffer ~pos ~len =
          let buffer =
            (* FIXME: I couldn't find an efficient conversion between
               Bigstringaf and Core.Bigstring *)
            Bigstringaf.of_string ~off:0 ~len:(Core.Bigstring.length buffer)
            @@ Core.Bigstring.to_string buffer
          in
          let read =
            H2.Client_connection.read client.connection buffer ~off:pos ~len
          in
          match H2.Client_connection.next_read_operation client.connection with
          | `Close -> Async.return @@ `Stop ()
          | `Read -> Async.return @@ `Consumed (read, `Need_unknown)
        in
        let close () = Async.return @@ Async.Socket.shutdown socket `Receive in
        match H2.Client_connection.next_read_operation client.connection with
        | `Close -> close ()
        | `Read ->
          let* _ =
            Async.Reader.read_one_chunk_at_a_time
              (Async.Reader.create (Async.Socket.fd socket))
              ~handle_chunk
          in
          close ()
      and write_connection =
        let writer = Async.Writer.create (Async.Socket.fd socket)
        and wait = Async.Ivar.create () in
        let rec write () =
          match H2.Client_connection.next_write_operation client.connection with
          | `Close _ ->
            let () = Async.Socket.shutdown socket `Send in
            Async.Ivar.fill wait ()
          | `Yield -> H2.Client_connection.yield_writer client.connection write
          | `Write vectors ->
            let () =
              let f l { Faraday.buffer; off = pos; len } =
                (* FIXME: I couldn't find an efficient conversion between
                   Bigstringaf and Core.Bigstring *)
                let buffer = Bigstringaf.to_string buffer in
                let () = Async.Writer.write ~pos ~len writer buffer in
                l + len
              in
              let written = List.fold_left ~f ~init:0 vectors in
              let () = debug [ Pp.textf "WRITTEN %d" written ] in
              H2.Client_connection.report_write_result client.connection
                (`Ok written)
            in
            write ()
        in
        let () = write () in
        Async.Ivar.read wait
      in
      let* (), () = Async.Deferred.both read_connection write_connection in
      Async.Unix.close (Async.Socket.fd socket)
    in
    Async.don't_wait_for io
  in
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

let call t target meth ?(headers = []) ?body path =
  let ( >>= ) = Async.( >>= )
  and ( >>| ) = Async.( >>| ) in
  let* client = client t target in
  let uri =
    Uri.with_uri ~path:(Option.some @@ Uri.path client.uri ^ path) client.uri
  in
  let request, body =
    let headers, body =
      match body with
      | Some body ->
        ( H2.Headers.of_list
          @@ (("Content-Type", "application/octet-stream") :: headers)
        , body )
      | None -> (H2.Headers.empty, Async.Pipe.of_list [])
    and scheme = Option.value_exn (Uri.scheme uri)
    and path = Uri.path uri in
    (H2.Request.create ~headers ~scheme meth path, body)
  in
  let result = Async.Ivar.create () in
  let response_handler response body =
    let reader, writer = Async.Pipe.create () in
    let rec on_eof () = Async.Pipe.close writer
    and on_read buffer ~off ~len =
      let bytes = Bytes.make len '\x00' in
      let () =
        Bigstringaf.blit_to_bytes buffer ~src_off:off bytes ~dst_off:0 ~len
      in
      let read =
        Async.Pipe.write writer (Bytes.unsafe_to_string bytes) >>| fun () ->
        H2.Body.schedule_read ~on_eof ~on_read body
      in
      Async.don't_wait_for read
    in
    let () = H2.Body.schedule_read ~on_eof ~on_read body in
    Async.Ivar.fill result @@ Result.return (response, reader)
  in
  let request =
    (* FIXME: error_handler *)
    H2.Client_connection.request client.connection request
      ~error_handler:(fun _ -> ())
      ~response_handler
  in
  let () =
    let rec send_chunk () =
      Async.Pipe.read body >>= function
      | `Eof -> Async.return @@ H2.Body.close_writer request
      | `Ok chunk ->
        let wait = Async.Ivar.create ()
        and () = H2.Body.write_string request chunk in
        let () = H2.Body.flush request (fun () -> Async.Ivar.fill wait ()) in
        Async.Ivar.read wait >>= fun () -> send_chunk ()
    in
    Async.don't_wait_for (send_chunk ())
  in
  let* response, body = Async.Ivar.read result in
  let drain () = Async.Pipe.drain body |> Async.Deferred.map ~f:Result.return in
  let* () =
    match H2.Headers.get response.headers "X-dune-hash" with
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
  Async.Deferred.Result.return (response, body)

let expect_status expected m path = function
  | effective when List.exists ~f:(Poly.( = ) effective) expected ->
    Result.Ok ()
  | effective ->
    Result.Error
      (Fmt.str "unexpected %s on %s %s"
         (H2.Status.to_string effective)
         (H2.Method.to_string m) path)

let put_contents t target path contents =
  let* response, body =
    let body = Async.Pipe.of_list [ contents ] in
    call t target `PUT ~body path
  in
  let* () = Async.Pipe.drain body |> Async.Deferred.map ~f:Result.return in
  Async.return @@ expect_status [ `Created; `OK ] `PUT path response.status

let get_file t target path local_path =
  if Path.exists local_path then
    debug
      [ Pp.textf "artifact already present locally: %s"
          (Path.to_string local_path)
      ]
    |> Async.Deferred.Result.return
  else
    let* response, body = call t target `GET path in
    let* () = expect_status [ `OK ] `GET path response.status |> Async.return in
    let executable =
      Poly.( = ) (H2.Headers.get response.headers "X-executable") (Some "1")
    in
    write_file t.cache local_path executable body

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
            let* status, body = call t digest meth ~headers ?body path in
            let* () =
              Async.Pipe.drain body |> Async.Deferred.map ~f:Result.return
            in
            Async.Deferred.Result.return status
          in
          let insert stats input =
            let body = Async.Reader.pipe input
            and headers =
              let executable =
                if stats.Unix.st_perm land 0o100 > 0 then
                  "1"
                else
                  "0"
              in
              [ ("X-executable", executable)
              ; ("Content-Length", Int.to_string stats.Unix.st_size)
              ]
            in

            query ~headers `PUT ~body
          in
          let stats = Path.stat path in
          let* upload =
            if stats.st_size < 4096 then
              Async.Deferred.Result.return true
            else
              let* response = query `HEAD in
              let* () =
                Async.return
                @@ expect_status [ `OK; `No_content ] `HEAD
                     (Path.to_string path) response.status
              in
              Async.Deferred.Result.return
                (Poly.( = ) response.status `No_content)
          in
          if upload then
            let* response =
              let path = Path.to_string path in
              let () = debug [ Pp.textf "distribute %S" path ] in
              Async.Reader.with_file path ~f:(insert stats)
            in
            Async.return
            @@ expect_status [ `Created; `OK ] `PUT (Path.to_string path)
                 response.status
          else
            Async.Deferred.Result.return ()
        in
        let ( let* ) = Async.Deferred.( >>= ) in
        let* results =
          Async.Deferred.List.map ~how:(`Max_concurrent_jobs 8) ~f:insert_file
            files
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

let string_of_body body =
  let+ body = Async.Pipe.read_all body |> Async.Deferred.map ~f:Result.return in
  Core_kernel.Queue.fold ~f:( ^ ) ~init:"" body

let prefetch ({ cache; _ } as t) count i key =
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
      let () = debug [ Pp.textf "fetch metadata %s (%i/%i)" hash i count ] in
      let* response, body =
        let* status, body = call t key `GET path in
        let* body = string_of_body body in
        Async.Deferred.Result.return (status, body)
      in
      let* () =
        expect_status [ `OK; `No_content ] `GET path response.status
        |> Async.return
      in
      if Poly.( = ) response.status `No_content then
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
          write_file cache local_path false (Async.Pipe.of_list [ body ])
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
    let* response, body = call t key `GET path in
    let* body =
      (* FIXME: parse the index file on the fly instead of dumping in to memory *)
      string_of_body body
    in
    let* () =
      expect_status [ `OK; `No_content ] `GET path response.status
      |> Async.return
    in
    if Poly.( = ) response.status `OK then
      let keys =
        String.split ~on:'\n' body
        |> List.filter_map ~f:(fun d -> Digest.from_hex d)
      in
      let count = List.length keys in
      let ( let* ) = Async.Deferred.( >>= ) in
      let* results =
        Async.Deferred.List.mapi ~how:(`Max_concurrent_jobs 8)
          ~f:(prefetch t count) keys
      in
      let ( let* ) = Async.Deferred.Result.( >>= ) in
      let* (_ : unit list) = Result.List.all results |> Async.return in
      Async.Deferred.Result.return ()
    else
      Async.Deferred.Result.return ()
  in
  let ( >>= ) = Async.Deferred.( >>= ) in
  let start = Unix.time () in
  Async.try_with ~extract_exn:true f >>= function
  | Result.Ok v ->
    let elapsed = Unix.time () -. start in
    let () =
      debug
        [ Pp.textf "prefetched commit %S in %f seconds" (Digest.to_string key)
            elapsed
        ]
    in

    Async.return v
  | Result.Error e ->
    Async.Deferred.Result.fail
      (Fmt.str "index_prefetch fatal error: %a" Exn.pp e)

let make config local =
  ( module struct
    let v =
      let local = Local.make local in
      make local config

    let distribute = distribute v

    let prefetch = prefetch v 1 1

    let index_add = index_add v

    let index_prefetch = index_prefetch v
  end : Distributed.S )
