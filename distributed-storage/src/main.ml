open Httpaf
open Stdune

type t = { root : Path.t }

let ( let* ) = Lwt.Infix.( >>= )

let request_handler { root } _ reqd =
  let { Request.meth; target; _ } = Reqd.request reqd in
  match meth with
  | `GET ->
    let respond () =
      try%lwt
        let path =
          Path.relative root
            (String.sub target ~pos:1 ~len:(String.length target - 1))
        in
        let stats = Path.stat path in
        let response =
          let headers =
            Headers.of_list
              [ ("Content-length", string_of_int stats.st_size)
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
        let f input =
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
          loop ()
        in
        let* () = Lwt_io.with_file ~mode:Lwt_io.input (Path.to_string path) f in
        Lwt.return @@ Body.close_writer body
      with
      | Unix.Unix_error (Unix.ENOENT, _, _)
      | Unix.Unix_error (Unix.EISDIR, _, _) ->
        let response = Response.create `Not_found in
        Lwt.return @@ Reqd.respond_with_string reqd response ""
    in
    Lwt.async respond
  | _ ->
    let headers = Headers.of_list [ ("Content-Length", "0") ] in
    Reqd.respond_with_string reqd
      (Response.create ~headers `Method_not_allowed)
      ""

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
