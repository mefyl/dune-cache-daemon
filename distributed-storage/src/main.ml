open Httpaf
open Stdune

type t = { root : Path.t }

let ( let* ) = Lwt.Infix.( >>= )

exception Bad_request of string

let error reqd status =
  let error reqd status reason =
    let* () =
      Logs_lwt.info (fun m -> m "> %s %s" (Status.to_string status) reason)
    in
    let contents = Format.sprintf "{\"reason\": %S}" reason in
    let headers =
      Headers.of_list
        [ ("Content-type", "application/json")
        ; ("Content-length", Int.to_string (String.length contents))
        ]
    in
    let response = Response.create ~headers status in
    Lwt.return @@ Reqd.respond_with_string reqd response contents
  in
  Format.ksprintf (error reqd status)

let bad_request = Format.ksprintf (fun reason -> raise (Bad_request reason))

let request_handler { root } _ reqd =
  let { Request.meth; target; _ } = Reqd.request reqd in
  let respond () =
    let* () =
      Logs_lwt.info (fun m -> m "%s %s" (Method.to_string meth) target)
    in
    try%lwt
      match String.split ~on:'/' target with
      | [ ""; "blocks"; hash ] -> (
        match meth with
        | `GET -> (
          let hash =
            match Digest.from_hex hash with
            | Some hash -> hash
            | None -> bad_request "invalid hash: %S" hash
          in
          let f stats input =
            let response =
              let headers =
                Headers.of_list
                  [ ("Content-length", string_of_int stats.Unix.st_size)
                  ; ("Content-type", "application/octet-stream")
                  ; ( "X-executable"
                    , if stats.st_perm land 0o100 <> 0 then
                        "1"
                      else
                        "0" )
                  ]
              in
              Response.create ~headers `OK
            in
            let body = Reqd.respond_with_streaming reqd response in
            let size = 4096 in
            let buffer = Bytes.make size '\x00' in
            let bigstring = Bigstringaf.create size in
            let rec loop () =
              let* read = Lwt_io.read_into input buffer 0 size in
              let () =
                Bigstringaf.unsafe_blit_from_bytes buffer ~src_off:0 bigstring
                  ~dst_off:0 ~len:read
              in
              let () = Body.schedule_bigstring body bigstring
              and wait, resolve = Lwt.wait () in
              let () = Body.flush body (Lwt.wakeup resolve) in
              let* () = wait in
              loop ()
            in
            let* () = loop () in
            Lwt.return @@ Body.close_writer body
          in
          try%lwt
            let path = Path.relative root (Digest.to_string hash) in
            let stats = Path.stat path in
            Lwt_io.with_file ~mode:Lwt_io.input (Path.to_string path) (f stats)
          with
          | Unix.Unix_error (Unix.ENOENT, _, _)
          | Unix.Unix_error (Unix.EISDIR, _, _) ->
            error reqd `Not_found "block %S not found locally"
              (Digest.to_string hash) )
        | _ ->
          let headers = Headers.of_list [ ("Content-Length", "0") ] in
          Lwt.return
          @@ Reqd.respond_with_string reqd
               (Response.create ~headers `Method_not_allowed)
               "" )
      | path ->
        error reqd `Bad_request "no such endpoint: %S"
          (String.concat ~sep:"/" path)
    with Bad_request reason -> error reqd `Bad_request "%s" reason
  in
  Lwt.async respond

let error_handler _ ?request:_ error start_response =
  let response_body = start_response Headers.empty in
  ( match error with
  | `Exn exn ->
    Body.write_string response_body (Printexc.to_string exn);
    Body.write_string response_body "\n"
  | #Status.standard as error ->
    Body.write_string response_body (Status.default_reason_phrase error) );
  Body.close_writer response_body

let port =
  Cmdliner.Arg.(
    value & opt int 80
    & info ~docv:"PORT" ~doc:"port to listen on" [ "p"; "port" ])

let root =
  Cmdliner.Arg.(
    required
    & opt (some string) None
    & info ~docv:"ROOT" ~doc:"root directory to server" [ "r"; "root" ])

let main port root =
  Lwt_main.run
  @@
  let t = { root = Path.of_string root } in
  let request_handler = request_handler t in
  let handler =
    Httpaf_lwt_unix.Server.create_connection_handler ~request_handler
      ~error_handler
  in
  let listen_address = Unix.(ADDR_INET (inet_addr_loopback, port)) in
  try%lwt
    let* _server =
      Lwt_io.establish_server_with_client_socket listen_address handler
    in
    let forever, _ = Lwt.wait () in
    forever
  with Unix.Unix_error (Unix.EACCES, "bind", _) ->
    Logs_lwt.err (fun m -> m "unable to bind to port %i" port)

let () =
  let () =
    Logs.set_reporter (Logs_fmt.reporter ());
    Logs.set_level (Some Logs.Debug)
  in
  let open Cmdliner in
  let doc = Term.info "dune-cache-distributed-storage" in
  Term.exit @@ Term.(eval (const main $ port $ root, doc))
