open Httpaf
open Stdune

type t = { root : Path.t }

open Lwt.Infix

let ( let* ) = ( >>= )

exception Bad_request of string

exception Method_not_allowed of Httpaf.Method.t

let status_to_string = function
  | `Code i -> Format.sprintf "%i" i
  | #Status.standard as status ->
    Format.sprintf "%s %s" (Status.to_string status)
      (Status.default_reason_phrase status)

let response reqd status =
  let* () = Logs_lwt.info (fun m -> m "> %s" (status_to_string status)) in
  let headers = Headers.of_list [ ("Content-length", "0") ] in
  let response = Response.create ~headers status in
  Lwt.return @@ Reqd.respond_with_string reqd response ""

let error reqd status =
  let error reqd status reason =
    let* () =
      Logs_lwt.info (fun m -> m "> %s: %s" (status_to_string status) reason)
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

module Blocks = struct
  let get { root } reqd hash =
    let f stats input =
      let file_size = stats.Unix.st_size in
      let response =
        let headers =
          Headers.of_list
            [ ("Content-length", string_of_int file_size)
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
        Lwt_io.read_into input buffer 0 size >>= function
        | 0 -> Lwt.return ()
        | read ->
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
      let* () =
        Logs_lwt.info (fun m -> m "> %s [%i bytes]" (status_to_string `OK) size)
      in
      Lwt.return @@ Body.close_writer body
    in
    try%lwt
      let path = Path.relative root (Digest.to_string hash) in
      let stats = Path.stat path in
      Lwt_io.with_file ~flags:[ Unix.O_RDONLY ] ~mode:Lwt_io.input
        (Path.to_string path) (f stats)
    with
    | Unix.Unix_error (Unix.ENOENT, _, _)
    | Unix.Unix_error (Unix.EISDIR, _, _) ->
      error reqd `Not_found "block %S not found locally" (Digest.to_string hash)

  let put { root } reqd hash executable =
    let f output =
      let wait, resolve = Lwt.wait () in
      let request_body = Reqd.request_body reqd in
      let size = 1024 in
      let buffer = Bytes.make size '\x00' in
      let rec on_read bs ~off ~len =
        let rec blit blit_from blit_end =
          if blit_from = blit_end then
            Lwt.return @@ Body.schedule_read request_body ~on_eof ~on_read
          else
            let len = min (blit_end - blit_from) size in
            let () =
              Bigstringaf.unsafe_blit_to_bytes bs ~src_off:blit_from buffer
                ~dst_off:0 ~len
            in
            let rec write write_from =
              if write_from = len then
                blit (blit_from + len) blit_end
              else
                let* written =
                  Lwt_io.write_from output buffer write_from (len - write_from)
                in
                write (write_from + written)
            in
            write 0
        in
        Lwt.async (fun () -> blit off (off + len))
      and on_eof () =
        Lwt.async (fun () ->
            let* () = response reqd `Created in
            Lwt.return @@ Lwt.wakeup resolve ())
      in
      let () = Body.schedule_read request_body ~on_eof ~on_read in
      wait
    in
    let path = Path.relative root (Digest.to_string hash)
    and perm =
      if executable then
        0o700
      else
        0o600
    in
    try%lwt
      Lwt_io.with_file
        ~flags:[ Unix.O_CREAT; Unix.O_TRUNC; Unix.O_WRONLY ]
        ~perm ~mode:Lwt_io.output (Path.to_string path) f
    with
    | Unix.Unix_error (Unix.ENOENT, _, _) ->
      error reqd `Not_found "block %S not found locally" (Digest.to_string hash)
    | Unix.Unix_error (Unix.EISDIR, _, _) ->
      let* () =
        Logs_lwt.err (fun m ->
            m "block file is a directory: %S" (Path.to_string path))
      in
      error reqd `Internal_server_error "unable to read block %S"
        (Digest.to_string hash)
    | Unix.Unix_error (Unix.EACCES, _, _) ->
      let* () =
        Logs_lwt.err (fun m ->
            m "permission denied on block: %S" (Path.to_string path))
      in
      error reqd `Internal_server_error "unable to read block %S"
        (Digest.to_string hash)
end

let string_of_sockaddr = function
  | Unix.ADDR_UNIX p -> p
  | Unix.ADDR_INET (addr, port) ->
    Format.sprintf "%s:%i" (Unix.string_of_inet_addr addr) port

let request_handler t sockaddr reqd =
  let { Request.meth; target; Request.headers; _ } = Reqd.request reqd in
  let respond () =
    let* () =
      Logs_lwt.info (fun m ->
          m "%s: %s %s"
            (string_of_sockaddr sockaddr)
            (Method.to_string meth) target)
    in
    let meth =
      match meth with
      | `GET -> `GET
      | `PUT -> `PUT
      | _ -> raise (Method_not_allowed meth)
    in
    try%lwt
      match String.split ~on:'/' target with
      | [ ""; "blocks"; hash ] -> (
        let hash =
          match Digest.from_hex hash with
          | Some hash -> hash
          | None -> bad_request "invalid hash: %S" hash
        in
        match meth with
        | `GET -> Blocks.get t reqd hash
        | `PUT ->
          let executable = Headers.get headers "X-executable" = Some "1" in
          Blocks.put t reqd hash executable )
      | [ ""; "index"; index; hash ] -> (
        let hash = Digest.string (index ^ hash) in
        match meth with
        | `GET -> Blocks.get t reqd hash
        | `PUT -> Blocks.put t reqd hash false )
      | path ->
        error reqd `Bad_request "no such endpoint: %S"
          (String.concat ~sep:"/" path)
    with
    | Bad_request reason -> error reqd `Bad_request "%s" reason
    | Method_not_allowed _ ->
      error reqd `Method_not_allowed "method not allowed"
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
  let () =
    try Unix.mkdir root 0o700 with Unix.Unix_error (Unix.EEXIST, _, _) -> ()
  in
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
