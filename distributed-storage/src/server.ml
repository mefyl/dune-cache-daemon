open Httpaf
open Stdune
open Utils
open Dune_cache_daemon

type t =
  { root : Path.t
  ; ranges : Config.range list
  }

exception Misdirected of Digest.t * Config.range list

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
  let check_range ranges address =
    if not (Config.ranges_include ranges address) then
      raise (Misdirected (address, ranges))

  let handle_errors hash path reqd f =
    try%lwt f with
    | Unix.Unix_error (Unix.ENOENT, _, _)
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

  let head { root; ranges } reqd hash =
    let () = check_range ranges hash in
    let path = Path.relative root (Digest.to_string hash) in
    let head =
      let stats = Path.stat path in
      let* () =
        Logs_lwt.info (fun m -> m "> %s [0 bytes]" (status_to_string `OK))
      in
      let response =
        let headers =
          Headers.of_list
            [ ("Content-length", "0")
            ; ( "X-executable"
              , if stats.st_perm land 0o100 <> 0 then
                  "1"
                else
                  "0" )
            ]
        in
        Response.create ~headers `OK
      in
      Lwt.return @@ Reqd.respond_with_string reqd response ""
    in
    handle_errors hash path reqd head

  let get { root; ranges } reqd hash =
    let () = check_range ranges hash in
    let path = Path.relative root (Digest.to_string hash) in
    let get =
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
          Logs_lwt.info (fun m ->
              m "> %s [%i bytes]" (status_to_string `OK) size)
        in
        Lwt.return @@ Body.close_writer body
      in
      let stats = Path.stat path in
      Lwt_io.with_file ~flags:[ Unix.O_RDONLY ] ~mode:Lwt_io.input
        (Path.to_string path) (f stats)
    in
    handle_errors hash path reqd get

  let put { root; ranges } reqd hash executable =
    let () = check_range ranges hash in
    let path = Path.relative root (Digest.to_string hash) in
    let put =
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
      let perm =
        if executable then
          0o700
        else
          0o600
      in
      Lwt_io.with_file
        ~flags:[ Unix.O_CREAT; Unix.O_TRUNC; Unix.O_WRONLY ]
        ~perm ~mode:Lwt_io.output (Path.to_string path) f
    in
    handle_errors hash path reqd put
end

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
      | `HEAD -> `HEAD
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
        | `HEAD -> Blocks.head t reqd hash
        | `GET -> Blocks.get t reqd hash
        | `PUT ->
          let executable = Headers.get headers "X-executable" = Some "1" in
          Blocks.put t reqd hash executable )
      | [ ""; "index"; index; hash ] -> (
        let hash = Digest.string (index ^ hash) in
        match meth with
        | `HEAD -> raise (Method_not_allowed meth)
        | `GET -> Blocks.get t reqd hash
        | `PUT -> Blocks.put t reqd hash false )
      | path ->
        error reqd `Bad_request "no such endpoint: %S"
          (String.concat ~sep:"/" path)
    with
    | Bad_request reason -> error reqd `Bad_request "%s" reason
    | Method_not_allowed _ ->
      error reqd `Method_not_allowed "method not allowed"
    | Misdirected (addr, _) ->
      error reqd (`Code 421) "address %s not in range" (Digest.to_string addr)
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

let trim { root; ranges } ~goal =
  let ( let* ) = Lwt.Infix.( >>= ) in
  let files =
    match Path.readdir_unsorted root with
    | Result.Ok l -> l
    | Result.Error e ->
      User_error.raise
        [ Pp.textf "unable to read storage root %s: %s" (Path.to_string root)
            (Unix.error_message e)
        ]
  in
  let f path =
    let* () = Lwt.pause () in
    let path = Path.relative root path in
    let* stats =
      try%lwt Lwt.return @@ Option.some @@ Path.stat path
      with Unix.Unix_error (e, _, _) ->
        let* () =
          Logs_lwt.warn (fun m ->
              m "unable to stat %s: %s" (Path.to_string path)
                (Unix.error_message e))
        in
        Lwt.return None
    in
    Lwt.return
    @@ Option.map
         ~f:(fun stats -> (path, Int64.of_int stats.st_size, stats.st_mtime))
         stats
  and compare (_, _, t1) (_, _, t2) = Ordering.of_int (Stdlib.compare t1 t2) in
  let* files = Lwt_list.filter_map_s f files in
  let* size =
    let f size (path, bytes, _) =
      match
        let ( let* ) v f = Option.bind ~f v in
        let basename = Path.basename path in
        let* hash = Digest.from_hex basename in
        Some (Config.ranges_include ranges hash)
      with
      | Some true -> Lwt.return @@ Int64.add size bytes
      | Some false ->
        let* () =
          Logs_lwt.warn (fun m ->
              m "remove out-of-range file: %s" (Path.to_string path))
        in
        Lwt.return size
      | None ->
        let* () =
          Logs_lwt.warn (fun m ->
              m "remove unrecognized file: %s" (Path.to_string path))
        in
        Lwt.return size
    in
    Lwt_list.fold_left_s f 0L files
  in
  if size > goal then
    let goal = Int64.sub size goal in
    let* () = Logs_lwt.info (fun m -> m "trimming %Li bytes" goal) in
    let files = List.sort ~compare files
    and delete (trimmed : int64) (path, bytes, _) =
      if trimmed >= goal then
        trimmed
      else
        let () = Path.unlink path in
        Int64.add trimmed bytes
    in
    let trimmed = List.fold_left ~init:0L ~f:delete files in
    Logs_lwt.info (fun m -> m "trimming freed %Li bytes" trimmed)
  else
    Logs_lwt.debug (fun m -> m "skip trimming")

let run config host port root trim_period trim_size =
  let ranges =
    match config with
    | None -> [ Config.range_total ]
    | Some path -> (
      match Config.of_file path with
      | Result.Ok c -> (
        let self = Uri.make ~host:(Unix.gethostname ()) ~port () in
        let f { Config.hostname; _ } =
          (* FIXME: also lookup ourselves by IP *)
          let hostname = Uri.with_scheme hostname None in
          Uri.equal hostname self
        in
        match List.find ~f c.nodes with
        | Some { space; _ } -> space
        | None ->
          User_error.raise
            [ Pp.textf "unable to find self (%s) in configuration file"
                (Uri.to_string self)
            ] )
      | Result.Error e ->
        User_error.raise
          [ Pp.textf "configuration error in %s: %s" (Path.to_string path) e ] )
  in
  let trim_period = float_of_int trim_period in
  Lwt_main.run
  @@
  let () =
    try Unix.mkdir (Path.to_string root) 0o700
    with Unix.Unix_error (Unix.EEXIST, _, _) -> ()
  in
  let t = { root; ranges } in
  let request_handler = request_handler t in
  let handler =
    Httpaf_lwt_unix.Server.create_connection_handler ~request_handler
      ~error_handler
  in
  let listen_address = Unix.(ADDR_INET (host, port)) in
  try%lwt
    let* _server =
      Lwt_io.establish_server_with_client_socket listen_address handler
    in
    let rec loop () =
      let* () = Lwt_unix.sleep trim_period in
      let* () = trim t ~goal:trim_size in
      loop ()
    in
    loop ()
  with Unix.Unix_error (Unix.EACCES, "bind", _) ->
    Logs_lwt.err (fun m -> m "unable to bind to port %i" port)
