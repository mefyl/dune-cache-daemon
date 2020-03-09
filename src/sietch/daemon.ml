module Evt = Event
module Utils = Utils
module Log = Dune_util.Log
open Stdune
open Utils
open Cache.Messages
open Result.O
open Pp.O

type client =
  { cache : Cache.Local.t
  ; common_metadata : Sexp.t list
  ; fd : Unix.file_descr
  ; input : char Stream.t
  ; output : out_channel
  ; peer : Unix.sockaddr
  ; version : version
  }

let default_port_file () =
  let runtime_dir =
    match Sys.getenv_opt "XDG_RUNTIME_DIR" with
    | Some p -> Path.relative (Path.of_string p) "dune-cache-daemon"
    | None ->
      (* The runtime directory is 0700 owned by the user for security reasons.
         Defaulting to a directory in the dune cache root makes sense in that
         regard, since if someone has access to this directory, it has access to
         the cache content, and having access to the socket does not make a
         difference. *)
      Path.relative (Cache.Local.default_root ()) "runtime"
  in
  Path.L.relative runtime_dir [ "dune-cache-daemon"; "port" ]

let max_port_size = 1024

let check_port_file ?(close = true) p =
  let p = Path.to_string p in
  match Result.try_with (fun () -> Unix.openfile p [ Unix.O_RDONLY ] 0o600) with
  | Result.Ok fd ->
    let f () =
      retry (fun () ->
          match Fcntl.lock_get fd Fcntl.Write with
          | None -> Some None
          | Some (Fcntl.Read, pid) -> Some (Some pid)
          | Some (Fcntl.Write, _) -> None)
      >>| Option.map ~f:(fun pid ->
              let buf = Bytes.make max_port_size ' ' in
              let read = Unix.read fd buf 0 max_port_size in
              (Bytes.sub_string buf ~pos:0 ~len:read, pid, fd))
    and finally () = if close then Unix.close fd in
    Exn.protect ~f ~finally
  | Result.Error (Unix.Unix_error (Unix.ENOENT, _, _)) -> Result.Ok None
  | Result.Error e -> Result.Error e

let send_sexp output sexp =
  output_string output (Csexp.to_string sexp);
  flush output

let send version output message =
  send_sexp output (sexp_of_message version message)

module ClientsKey = struct
  type t = Unix.file_descr

  let compare a b = Ordering.of_int (Stdlib.compare a b)

  let to_dyn _ = Dyn.Opaque
end

module Clients = Map.Make (ClientsKey)

type config = { exit_no_client : bool }

type event =
  | Stop
  | New_client of Unix.file_descr * Unix.sockaddr
  | Client_left of Unix.file_descr

type t =
  { root : Path.t option
  ; mutable socket : Unix.file_descr option
  ; mutable clients : (client * Thread.t) Clients.t
  ; mutable endpoint : string option
  ; mutable accept_thread : Thread.t option
  ; mutable trim_thread : Thread.t option
  ; config : config
  ; events : event Evt.channel
  ; cache : Cache.Local.t
  ; distributed : (module Distributed.S)
  }

exception Error of string

let make ?root ~config () : t =
  match
    Cache.Local.make ?root ~duplication_mode:Cache.Duplication_mode.Hardlink
      (fun _ -> ())
  with
  | Result.Error msg -> User_error.raise [ Pp.text msg ]
  | Result.Ok cache ->
    { root
    ; socket = None
    ; clients = Clients.empty
    ; endpoint = None
    ; accept_thread = None
    ; trim_thread = None
    ; config
    ; events = Evt.new_channel ()
    ; cache
    ; distributed = Distributed.irmin cache
    }

let getsockname = function
  | Unix.ADDR_UNIX _ ->
    User_error.raise
      [ Pp.textf "got a Unix socket connection on our TCP socket ?" ]
  | Unix.ADDR_INET (addr, port) -> (addr, port)

let peer_name s =
  let addr, port = getsockname s in
  Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port

let stop daemon = Evt.sync (Evt.send daemon.events Stop)

let my_versions : version list = [ { major = 1; minor = 2 } ]

let endpoint m = m.endpoint

let client_handle version output = function
  | Cache.Dedup f -> send version output (Dedup f)

let client_thread (daemon, (client : client)) =
  let events = daemon.events in
  try
    let handle_cmd (client : client) sexp =
      let* msg = outgoing_message_of_sexp client.version sexp in
      match msg with
      | Hint keys ->
        let+ () =
          let module D = (val daemon.distributed) in
          let f k = D.prefetch k in
          Result.List.iter ~f keys
        in
        client
      | Promote { duplication; repository; files; key; metadata } ->
        let metadata = metadata @ client.common_metadata in
        let+ metadata, _ =
          Cache.Local.promote_sync client.cache files key metadata ~repository
            ~duplication
        in
        let () =
          (* distribute *)
          let module D = (val daemon.distributed) in
          match D.distribute key metadata with
          | Result.Ok () -> ()
          | Result.Error e -> Log.info [ Pp.textf "failed to distribute: %s" e ]
        in
        client
      | SetBuildRoot root ->
        Result.Ok
          { client with cache = Cache.Local.set_build_dir client.cache root }
      | SetCommonMetadata metadata ->
        Result.ok { client with common_metadata = metadata }
      | SetRepos repositories ->
        let cache = Cache.Local.with_repositories client.cache repositories in
        Result.Ok { client with cache }
    in
    let input = client.input in
    let f () =
      let rec handle client =
        match Stream.peek input with
        | None -> Log.info [ Pp.textf "%s: ended" (peer_name client.peer) ]
        | Some '\n' ->
          (* Skip toplevel newlines, for easy netcat interaction *)
          Stream.junk input;
          (handle [@tailcall]) client
        | _ -> (
          match
            let* cmd =
              Result.map_error
                ~f:(fun r -> "parse error: " ^ r)
                (Csexp.parse input)
            in
            Log.info
              [ Pp.box ~indent:2
                @@ Pp.textf "%s: received command" (peer_name client.peer)
                   ++ Pp.space
                   ++ Pp.hbox (Pp.text @@ Sexp.to_string cmd)
              ];
            handle_cmd client cmd
          with
          | Result.Error e ->
            Log.info
              [ Pp.textf "%s: command error: %s" (peer_name client.peer) e ];
            handle client
          | Result.Ok client -> handle client )
      in
      handle client
    and finally () =
      ( try
          Unix.shutdown client.fd Unix.SHUTDOWN_ALL;
          Unix.close client.fd
        with Unix.Unix_error (Unix.ENOTCONN, _, _) -> () );
      Cache.Local.teardown client.cache;
      Evt.sync (Evt.send events (Client_left client.fd))
    in
    try Exn.protect ~f ~finally with
    | Unix.Unix_error (Unix.EBADF, _, _) ->
      Log.info [ Pp.textf "%s: ended" (peer_name client.peer) ]
    | Sys_error msg ->
      Log.info [ Pp.textf "%s: ended: %s" (peer_name client.peer) msg ]
  with Code_error.E e as exn ->
    Log.info
      [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
          (Dyn.to_string (Code_error.to_dyn e))
      ];
    raise exn

let run ?(port_f = ignore) ?(port = 0) daemon =
  let trim_thread max_size period cache =
    let rec trim () =
      Unix.sleep period;
      let () =
        match
          let size = Cache.Local.size cache in
          if size > max_size then (
            Log.info [ Pp.textf "trimming %i bytes" (size - max_size) ];
            Some (Cache.Local.trim cache (size - max_size))
          ) else
            None
        with
        | Some { trimmed_files_size = freed; _ } ->
          Log.info [ Pp.textf "trimming freed %i bytes" freed ]
        | None -> Log.info [ Pp.textf "skip trimming" ]
      in
      trim ()
    in
    trim ()
  in
  let rec accept_thread sock =
    let rec accept () =
      try Unix.accept sock
      with Unix.Unix_error (Unix.EINTR, _, _) -> (accept [@tailcall]) ()
    in
    let fd, peer = accept () in
    ( try Evt.sync (Evt.send daemon.events (New_client (fd, peer)))
      with Unix.Unix_error (Unix.EBADF, _, _) -> () );
    (accept_thread [@tailcall]) sock
  in
  let f () =
    let+ trim_period =
      Option.value
        ~default:(Result.Ok (10 * 60))
        (Option.map ~f:int_of_string
           (Env.get Env.initial "DUNE_CACHE_TRIM_PERIOD"))
    and+ trim_size =
      Option.value
        ~default:(Result.Ok (10 * 1024 * 1024 * 1024))
        (Option.map ~f:int_of_string
           (Env.get Env.initial "DUNE_CACHE_TRIM_SIZE"))
    in
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    daemon.socket <- Some sock;
    Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", port));
    let addr, port = getsockname (Unix.getsockname sock) in
    let endpoint =
      Printf.sprintf "%s:%i" (Unix.string_of_inet_addr addr) port
    in
    daemon.endpoint <- Some endpoint;
    port_f endpoint;
    Unix.listen sock 1024;
    daemon.accept_thread <- Some (Thread.create accept_thread sock);
    daemon.trim_thread <-
      Some (Thread.create (trim_thread trim_size trim_period) daemon.cache);
    let rec handle () =
      let stop () =
        let () =
          match daemon.socket with
          | Some fd ->
            daemon.socket <- None;
            let clean f = ignore (Clients.iter ~f daemon.clients) in
            clean (fun (client, _) -> Unix.shutdown client.fd Unix.SHUTDOWN_ALL);
            clean (fun (_, tid) -> Thread.join tid);
            clean (fun (client, _) -> Unix.close client.fd);
            Unix.close fd
          | _ -> Log.info [ Pp.textf "stop" ]
        in
        Cache.Local.teardown daemon.cache
      in
      ( match Evt.sync (Evt.receive daemon.events) with
      | Stop -> stop ()
      | New_client (fd, peer) -> (
        let output = Unix.out_channel_of_descr fd
        and input = Stream.of_channel (Unix.in_channel_of_descr fd) in
        match
          Log.info [ Pp.textf "accept new client %s" (peer_name peer) ];
          let* version = negotiate_version my_versions fd input output in
          Log.info
            [ Pp.textf "negotiated protocol version %s"
              @@ Cache.Messages.string_of_version version
            ];
          let client =
            let cache =
              match
                Cache.Local.make ?root:daemon.root
                  ~duplication_mode:Cache.Duplication_mode.Hardlink
                  (client_handle version output)
              with
              | Result.Ok m -> m
              | Result.Error e -> User_error.raise [ Pp.textf "%s" e ]
            in
            { cache; common_metadata = []; fd; input; output; peer; version }
          in
          let tid = Thread.create client_thread (daemon, client) in
          let+ clients =
            Result.map_error
              ~f:(fun _ -> "duplicate socket")
              (Clients.add daemon.clients client.fd (client, tid))
          in
          daemon.clients <- clients
        with
        | Result.Ok () -> ()
        | Result.Error msg -> Log.info [ Pp.textf "reject client: %s" msg ] )
      | Client_left fd ->
        daemon.clients <- Clients.remove daemon.clients fd;
        if daemon.config.exit_no_client && Clients.is_empty daemon.clients then
          stop () );
      if Option.is_some daemon.socket then (handle [@tailcall]) ()
    in
    handle ()
  in
  match f () with
  | Result.Ok () -> ()
  | Result.Error msg -> User_error.raise [ Pp.text msg ]
  | exception Unix.Unix_error (errno, f, _) ->
    User_error.raise
      [ Pp.textf "unable to %s: %s\n" f (Unix.error_message errno) ]

let () =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (Logs.format_reporter ())

let daemon ~root ~config started =
  Path.mkdir_p root;
  let daemon = make ~root ~config () in
  (* Event blocks signals when waiting. Use a separate thread to catch signals. *)
  let signal_handler s =
    Log.info [ Pp.textf "caught signal %i, exiting" s ];
    ignore (Thread.create stop daemon)
  and signals = [ Sys.sigint; Sys.sigterm ] in
  let rec signals_handler () =
    signal_handler (Thread.wait_signal signals);
    signals_handler ()
  in
  ignore (Thread.sigmask Unix.SIG_BLOCK signals);
  ignore (Thread.create signals_handler ());
  try run ~port_f:started daemon
  with Error s ->
    Printf.fprintf stderr "%s: fatal error: %s\n%!" Sys.argv.(0) s;
    exit 1
