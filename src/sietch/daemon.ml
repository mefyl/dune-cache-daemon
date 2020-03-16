module Evt = Event
module Utils = Utils
module Log = Dune_util.Log
module Csexp_sietch = Csexp
open Stdune
open Utils
open Cache.Messages
open Result.O
open Pp.O

type error =
  [ `Local_cache_error of string
  | `Fatal of string
  ]

module LwtR = struct
  let ( let* ) = Lwt_result.Infix.( >>= )
end

type client =
  { cache : Cache.Local.t
  ; common_metadata : Sexp.t list
  ; fd : Lwt_unix.file_descr
  ; input : Csexp_sietch.Generic.Lwt_istream.t
  ; output : Csexp_sietch.Generic.Lwt_ostream.t
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
  let open LwtR in
  let* () = Csexp_sietch.Generic.Parser_lwt.serialize output sexp in
  try%lwt Lwt_io.flush output |> Lwt.map Result.ok
  with Unix.Unix_error (e, f, _) ->
    Lwt_result.fail
      (`Write_error (Printf.sprintf "%s: %s" (Unix.error_message e) f))

let send version output message =
  send_sexp output (sexp_of_message version message)

type fd = Lwt_unix.file_descr

module ClientsKey = struct
  type t = fd

  let compare a b = Ordering.of_int (Stdlib.compare a b)

  let to_dyn _ = Dyn.Opaque
end

module Clients = Map.Make (ClientsKey)

type config = { exit_no_client : bool }

type event =
  | Stop
  | New_client of fd * Unix.sockaddr
  | Client_left of fd

type t =
  { root : Path.t
  ; mutable socket : fd option
  ; mutable clients : (client * unit Lwt.t) Clients.t
  ; mutable endpoint : string option
  ; mutable accept_thread : unit Lwt.t option
  ; mutable trim_thread : unit Lwt.t option
  ; config : config
  ; events : event Lwt_stream.t
  ; event_push : event option -> unit
  ; cache : Cache.Local.t
  ; distributed : (module Distributed.S)
  }

exception Error of string

let make ?root ~config () =
  match
    Cache.Local.make ?root ~duplication_mode:Cache.Duplication_mode.Hardlink
      (fun _ -> ())
  with
  | Result.Error msg -> Lwt_result.fail (`Local_cache_error msg)
  | Result.Ok cache ->
    let events, event_push = Lwt_stream.create () in
    Lwt_result.return
      { root = Option.value root ~default:(Cache.Local.default_root ())
      ; socket = None
      ; clients = Clients.empty
      ; endpoint = None
      ; accept_thread = None
      ; trim_thread = None
      ; config
      ; events
      ; event_push
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

let my_versions : version list = [ { major = 1; minor = 2 } ]

let endpoint m = m.endpoint

let client_handle peer version output = function
  | Cache.Dedup f ->
    let send () =
      let sent = send version output (Dedup f)
      and f = function
        | Result.Ok () -> ()
        | Result.Error (`Write_error e) ->
          Log.info [ Pp.textf "%s: write error: %s" (peer_name peer) e ]
      in
      Lwt.map f sent
    in
    (* Because the Cache.Local expect a -> unit handler, we have to run this
       asynchronously *)
    (* FIXME: Hence we should probably lock. *)
    Lwt.async send

let protect ~f ~finally =
  let open LwtO in
  try
    let* res = f () in
    let+ () = finally () in
    res
  with e ->
    let* () = finally () in
    raise e

let outgoing_message_of_sexp version sexp =
  outgoing_message_of_sexp version sexp
  |> Result.map_error ~f:(fun s -> `Protocol_error s)

let close_client_socket fd =
  let () =
    try Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL
    with Unix.Unix_error (Unix.ENOTCONN, _, _) -> ()
  in
  Lwt_unix.close fd

let client_thread (daemon, (client : client)) =
  try
    let handle_cmd (client : client) sexp =
      let log_error op = function
        | Result.Ok () -> ()
        | Result.Error e ->
          Log.info [ Pp.textf "%s: %s error: %s" (peer_name client.peer) op e ]
      in
      let open LwtR in
      let* msg = outgoing_message_of_sexp client.version sexp |> Lwt.return in
      match msg with
      | Hint keys ->
        let open LwtO in
        let module D = (val daemon.distributed) in
        let f k = D.prefetch k in
        let prefetching () =
          let* results = Lwt_list.map_s f keys in
          List.iter ~f:(log_error "distribute") results |> Lwt.return
        in
        Lwt.async prefetching;
        Lwt_result.return client
      | Promote { duplication; repository; files; key; metadata } ->
        let metadata = metadata @ client.common_metadata in
        let* () =
          let* metadata, _ =
            Cache.Local.promote_sync client.cache files key metadata ~repository
              ~duplication
            |> Result.map_error ~f:(fun e -> `Distribution_error e)
            |> Lwt.return
          in
          (* distribute *)
          let module D = (val daemon.distributed) in
          let%lwt res = D.distribute key metadata in
          res |> log_error "distribute" |> Lwt_result.return
        in
        Lwt_result.return client
      | SetBuildRoot root ->
        let res =
          let open Result.O in
          let+ cache = Cache.Local.set_build_dir client.cache root in
          { client with cache }
        in
        res |> Result.map_error ~f:(fun e -> `Local_cache_error e) |> Lwt.return
      | SetCommonMetadata metadata ->
        Lwt_result.return { client with common_metadata = metadata }
      | SetRepos repositories ->
        let cache = Cache.Local.with_repositories client.cache repositories in
        Lwt_result.return { client with cache }
    in
    let input = client.input in
    let f () =
      let rec handle client =
        let open Lwt_result.Infix in
        let open Csexp_sietch.Generic.Lwt_istream in
        let open LwtR in
        peek input >>= function
        | None ->
          Log.info [ Pp.textf "%s: ended" (peer_name client.peer) ];
          Lwt_result.return client
        | Some '\n' ->
          (* Skip toplevel newlines, for easy netcat interaction *)
          let* _ = next input in
          (handle [@tailcall]) client
        | _ ->
          let* cmd = Csexp_sietch.Generic.Parser_lwt.parse input in
          Log.info
            [ Pp.box ~indent:2
              @@ Pp.textf "%s: received command" (peer_name client.peer)
                 ++ Pp.space
                 ++ Pp.hbox (Pp.text @@ Sexp.to_string cmd)
            ];
          let* client = handle_cmd client cmd in
          (handle [@tailcall]) client
      in
      handle client
    and finally () =
      let open LwtO in
      let+ () = close_client_socket client.fd in
      let () = Cache.Local.teardown client.cache in
      daemon.event_push (Some (Client_left client.fd))
    in
    try protect ~f ~finally with
    | Unix.Unix_error (Unix.EBADF, _, _) ->
      Log.info [ Pp.textf "%s: ended" (peer_name client.peer) ];
      Lwt_result.return client
    | Sys_error msg ->
      Log.info [ Pp.textf "%s: ended: %s" (peer_name client.peer) msg ];
      Lwt_result.return client
  with Code_error.E e as exn ->
    Log.info
      [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
          (Dyn.to_string (Code_error.to_dyn e))
      ];
    raise exn

let run ?(port_f = ignore) ?(port = 0) ?(trim_period = 10 * 60)
    ?(trim_size = 10 * 1024 * 1024 * 1024) daemon =
  let trim_thread max_size period cache =
    let period = float_of_int period in
    let open LwtO in
    let rec trim () =
      let* () = Lwt_unix.sleep period in
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
      try Lwt_unix.accept sock
      with Unix.Unix_error (Unix.EINTR, _, _) -> (accept [@tailcall]) ()
    in
    let open LwtO in
    let* fd, peer = accept () in
    daemon.event_push (Some (New_client (fd, peer)));
    (accept_thread [@tailcall]) sock
  in
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  daemon.socket <- Some sock;
  let open LwtO in
  let* () =
    Lwt_unix.bind sock
      (Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", port))
  in
  let addr, port = getsockname (Lwt_unix.getsockname sock) in
  let endpoint = Printf.sprintf "%s:%i" (Unix.string_of_inet_addr addr) port in
  daemon.endpoint <- Some endpoint;
  port_f endpoint;
  Lwt_unix.listen sock 1024;
  daemon.accept_thread <- Some (accept_thread sock);
  daemon.trim_thread <- Some (trim_thread trim_size trim_period daemon.cache);
  let rec handle () =
    let stop () =
      Log.info [ Pp.textf "stop" ];
      let+ () =
        match daemon.socket with
        | Some fd ->
          daemon.socket <- None;
          let clean f = Lwt_list.iter_p f (Clients.to_list daemon.clients) in
          let* () =
            clean (fun (fd, _) ->
                Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL |> Lwt.return)
          in
          let* () = clean (fun (_, (_, thread)) -> thread) in
          let* () = clean (fun (fd, _) -> Lwt_unix.close fd) in
          Lwt_unix.close fd
        | None -> Lwt.return ()
      in
      Cache.Local.teardown daemon.cache
    in
    Lwt_stream.get daemon.events >>= function
    | None
    | Some Stop ->
      stop ()
    | Some (New_client (fd, peer)) ->
      let input =
        Lwt_io.of_fd ~mode:Lwt_io.input fd
        |> Csexp_sietch.Generic.Lwt_istream.make
      and output =
        Lwt_io.of_fd ~mode:Lwt_io.output fd
        |> Csexp_sietch.Generic.Lwt_ostream.make
      in
      let* res =
        Log.info [ Pp.textf "accept new client %s" (peer_name peer) ];
        let open LwtR in
        let* () =
          let version_message =
            Lang my_versions
            |> Cache.Messages.sexp_of_message { major = 1; minor = 0 }
          in
          Csexp_sietch.Generic.Parser_lwt.serialize output version_message
        in
        let* their_versions =
          let* msg = Csexp_sietch.Generic.Parser_lwt.parse input in
          initial_message_of_sexp msg
          |> Result.map ~f:(fun (Lang res) -> res)
          |> Result.map_error ~f:(fun e -> `Protocol_error e)
          |> Lwt.return
        in
        let* version =
          highest_common_version my_versions their_versions
          |> Result.map_error ~f:(fun _ ->
                 `Version_mismatch (my_versions, their_versions))
          |> Lwt.return
        in
        Log.info
          [ Pp.textf "negotiated protocol version %s"
            @@ Cache.Messages.string_of_version version
          ];
        let client =
          let cache =
            match
              Cache.Local.make ~root:daemon.root
                ~duplication_mode:Cache.Duplication_mode.Hardlink
                (client_handle peer version output)
            with
            | Result.Ok m -> m
            | Result.Error e -> User_error.raise [ Pp.textf "%s" e ]
          in
          { cache; common_metadata = []; fd; input; output; peer; version }
        in
        let thread =
          let open Lwt.Infix in
          client_thread (daemon, client) >|= function
          | Result.Ok _ -> ()
          | Result.Error (`Distribution_error e)
          | Result.Error (`Local_cache_error e)
          | Result.Error (`Parse_error e)
          | Result.Error (`Protocol_error e)
          | Result.Error (`Read_error e) ->
            Log.info [ Pp.textf "%s: client error: %s" (peer_name peer) e ]
        in
        match Clients.add daemon.clients client.fd (client, thread) with
        | Result.Ok clients ->
          daemon.clients <- clients;
          Lwt_result.return ()
        | Result.Error _ -> Code_error.raise "duplicate socket" []
      in
      let* () =
        match res with
        | Result.Ok () -> Lwt.return ()
        | Result.Error e ->
          ( match e with
          | `Parse_error msg
          | `Protocol_error msg
          | `Read_error msg
          | `Write_error msg ->
            Log.info [ Pp.textf "reject client: %s" msg ]
          | `Version_mismatch _ ->
            Log.info [ Pp.textf "reject client: version mismatch" ] );
          close_client_socket fd
      in
      (handle [@tailcall]) ()
    | Some (Client_left fd) ->
      daemon.clients <- Clients.remove daemon.clients fd;
      let* () =
        if daemon.config.exit_no_client && Clients.is_empty daemon.clients then
          stop ()
        else
          Lwt.return ()
      in
      if Option.is_some daemon.socket then
        (handle [@tailcall]) ()
      else
        Lwt.return ()
  in
  handle ()

let () = Logs.set_reporter (Logs.format_reporter ())

let daemon ~root ~config started =
  let sietch =
    let open LwtR in
    Path.mkdir_p root;
    let* daemon = make ~root ~config () in
    (* Event blocks signals when waiting. Use a separate thread to catch
       signals. *)
    let signal_handler s =
      Log.info [ Pp.textf "caught signal %i, exiting" (-s) ];
      daemon.event_push (Some Stop)
    and signals = [ Sys.sigint; Sys.sigterm ] in
    let _ =
      let f s = Lwt_unix.on_signal s signal_handler in
      List.map ~f signals
    in
    let ran = run ~port_f:started daemon in
    Lwt.map Result.ok ran
  in
  match Lwt_main.run sietch with
  | Result.Ok () -> ()
  | Result.Error (`Fatal e)
  | Result.Error (`Local_cache_error e) ->
    Printf.fprintf stderr "%s: fatal error: %s\n%!" Sys.argv.(0) e;
    exit 1
