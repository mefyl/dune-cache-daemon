module Evt = Event
module Utils = Utils
module Log = Dune_util.Log
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
  ; commits : Digest.t list array
  ; common_metadata : Sexp.t list
  ; fd : Lwt_unix.file_descr
  ; input : Csexp.Lwt.Input.t
  ; output : Lwt_io.output_channel
  ; peer : Unix.sockaddr
  ; repositories : Cache.repository list
  ; version : version
  }

let info = Log.info

let debug = ignore

let default_runtime_file name =
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
  Path.L.relative runtime_dir [ "dune-cache-daemon"; name ]

let default_endpoint_file () = default_runtime_file "endpoint"

let max_endpoint_size = 1024

let check_endpoint_file ?(close = true) p =
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
              let buf = Bytes.make max_endpoint_size ' ' in
              let read = Unix.read fd buf 0 max_endpoint_size in
              (Bytes.sub_string buf ~pos:0 ~len:read, pid, fd))
    and finally () = if close then Unix.close fd in
    Exn.protect ~f ~finally
  | Result.Error (Unix.Unix_error (Unix.ENOENT, _, _)) -> Result.Ok None
  | Result.Error e -> Result.Error e

let send_sexp output sexp =
  let open LwtR in
  let* () =
    Lwt_io.write output @@ Csexp.to_string sexp |> Lwt.map Result.return
  in
  try%lwt Lwt_io.flush output |> Lwt.map Result.return
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
  ; mutable endpoint : Uri.t option
  ; mutable accept_thread : unit Lwt.t option
  ; mutable trim_thread : unit Lwt.t option
  ; config : config
  ; events : event Lwt_stream.t
  ; event_push : event option -> unit
  ; cache : Cache.Local.t
  ; distributed : (module Distributed.S)
  ; distribution : (Cache.Key.t * Cache.Local.Metadata_file.t) Lwt_stream.t
  ; distribute : (Cache.Key.t * Cache.Local.Metadata_file.t) option -> unit
  ; distribution_barrier : Barrier.t
  }

exception Error of string

let log_error context op = function
  | Result.Ok () -> ()
  | Result.Error e -> info [ Pp.textf "%s: %s error: %s" context op e ]

let make ?root ?(distribution = Distributed.disabled) ~config () =
  match
    Cache.Local.make ?root ~duplication_mode:Cache.Duplication_mode.Hardlink
      ~command_handler:(fun _ -> ())
      ()
  with
  | Result.Error msg -> Lwt_result.fail (`Local_cache_error msg)
  | Result.Ok cache ->
    let events, event_push = Lwt_stream.create ()
    and distributed = distribution cache
    and distribution, distribute = Lwt_stream.create ()
    and distribution_barrier = Barrier.make () in
    let rec distribution_thread () =
      let%lwt () = Barrier.wait distribution_barrier in
      let%lwt was_stopped =
        let%lwt empty = Lwt_stream.is_empty distribution in
        if empty then
          let () = info [ Pp.textf "done distributing" ] in
          Lwt.return true
        else
          Lwt.return false
      in
      match%lwt Lwt_stream.get distribution with
      | Some (key, metadata) ->
        if was_stopped then info [ Pp.textf "start distributing" ];
        let module D = (val distributed) in
        let%lwt res = D.distribute key metadata in
        log_error (Digest.to_string key) "distribute" res;
        distribution_thread ()
      | None -> Lwt.return ()
    in
    Lwt.async distribution_thread;
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
      ; distributed
      ; distribution
      ; distribute
      ; distribution_barrier
      }

let getsockname = function
  | Unix.ADDR_UNIX path -> Uri.make ~scheme:"unix" ~path ()
  | Unix.ADDR_INET (addr, port) ->
    Uri.make ~scheme:"tcp" ~host:(Unix.string_of_inet_addr addr) ~port ()

let peer_name s = getsockname s |> Uri.to_string

let my_versions : version list = [ { major = 1; minor = 2 } ]

let endpoint m = m.endpoint |> Option.map ~f:Uri.to_string

let client_handle peer version output = function
  | Cache.Dedup f ->
    let send () =
      let sent = send version output (Dedup f)
      and f = function
        | Result.Ok () -> ()
        | Result.Error (`Write_error e) ->
          info [ Pp.textf "%s: write error: %s" (peer_name peer) e ]
      in
      Lwt.map f sent
    in
    (* Because the Cache.Local expect a -> unit handler, we have to run this
       asynchronously *)
    (* FIXME: Hence we should probably lock. *)
    Lwt.async send

let protect ~f ~finally =
  let open LwtO in
  try%lwt
    let* res = f () in
    let+ () = finally () in
    res
  with e ->
    let* () = finally () in
    Lwt.fail e

let outgoing_message_of_sexp version sexp =
  outgoing_message_of_sexp version sexp
  |> Result.map_error ~f:(fun s -> `Protocol_error s)

let close_client_socket fd =
  let () =
    try Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL
    with Unix.Unix_error (Unix.ENOTCONN, _, _) -> ()
  in
  Lwt_unix.close fd

let commits_index_key = "commits"

let index_commits { distributed = (module Distributed); _ }
    ({ commits; repositories; _ } as client) =
  let open LwtrO in
  let f i (repository : Cache.repository) =
    let commits = commits.(i)
    and key = Digest.string repository.commit in
    Distributed.index_add commits_index_key key commits
  in
  let%lwt results = Lwt_list.mapi_p f repositories in
  let+ () =
    Result.List.all results
    |> Result.map ~f:(fun (_ : unit list) -> ())
    |> Result.map_error ~f:(fun e -> `Distribution_error e)
    |> Lwt.return
  in
  { client with commits = [||]; repositories = [] }

let client_thread daemon client =
  try
    let handle_cmd (client : client) sexp () =
      let open LwtR in
      let* msg = outgoing_message_of_sexp client.version sexp |> Lwt.return in
      let promoted metadata { repository; key; _ } =
        let () = daemon.distribute (Some (key, metadata)) in
        let register_commit repository =
          client.commits.(repository) <- key :: client.commits.(repository)
        in
        Option.iter ~f:register_commit repository
      in
      match msg with
      | Hint keys ->
        let open LwtO in
        let module D = (val daemon.distributed) in
        let f k = D.prefetch k in
        let prefetching () =
          let* results = Lwt_list.map_s f keys in
          List.iter ~f:(log_error (peer_name client.peer) "distribute") results
          |> Lwt.return
        in
        Lwt.async prefetching;
        Lwt_result.return client
      | Promote ({ duplication; repository; files; key; metadata } as promotion)
        ->
        let metadata = metadata @ client.common_metadata in
        let* metadata, _ =
          Cache.Local.promote_sync client.cache files key metadata ~repository
            ~duplication
          |> Result.map_error ~f:(fun e -> `Distribution_error e)
          |> Lwt.return
        in
        let () = promoted metadata promotion in
        Lwt_result.return client
      | Promoted ({ key; _ } as promotion) ->
        let* metadata =
          let* sexp =
            Cache.Local.metadata_path client.cache key
            |> Io.read_file |> Csexp.parse_string
            |> Result.map_error ~f:(fun (_, e) -> `Parse_error e)
            |> Lwt.return
          in
          Cache.Local.Metadata_file.of_sexp sexp
          |> Result.map_error ~f:(fun e -> `Parse_error e)
          |> Lwt.return
        in
        let () = promoted metadata promotion in
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
        let prefetching () =
          let module D = (val daemon.distributed) in
          let f (repository : Cache.repository) =
            D.index_prefetch commits_index_key (Digest.string repository.commit)
          in
          let%lwt results = Lwt_list.map_p f repositories in
          match Result.List.all results with
          | Result.Ok (_ : unit list) -> Lwt.return ()
          | Result.Error e ->
            info
              [ Pp.textf "error while prefetching index %s: %s"
                  commits_index_key e
              ];
            Lwt.return ()
        in
        let () = Lwt.async prefetching in
        let* client = index_commits daemon client in
        let* cache =
          Cache.Local.with_repositories client.cache repositories
          |> Result.map_error ~f:(fun s -> `Remote_cache_error s)
          |> Lwt.return
        in
        let commits = Array.make (List.length repositories) [] in
        Lwt_result.return { client with cache; commits; repositories }
    in
    let input = client.input in
    let f () =
      let rec handle client =
        let open Lwt_result.Infix in
        let open Csexp.Lwt.Input in
        let open LwtR in
        debug [ Pp.textf "%s: read next command" (peer_name client.peer) ];
        peek_char input |> Lwt_result.map_err (fun e -> `Read_error e)
        >>= function
        | None ->
          debug [ Pp.textf "%s: ended" (peer_name client.peer) ];
          let* client = index_commits daemon client in
          Lwt_result.return client
        | Some '\n' ->
          (* Skip toplevel newlines, for easy netcat interaction *)
          let* _ =
            read_char input |> Lwt_result.map_err (fun e -> `Read_error e)
          in
          (handle [@tailcall]) client
        | _ ->
          let* cmd = Csexp.Lwt.Parser.parse input in
          debug
            [ Pp.box ~indent:2
              @@ Pp.textf "%s: received command" (peer_name client.peer)
                 ++ Pp.space
                 ++ Pp.hbox (Pp.text @@ Sexp.to_string cmd)
            ];
          let* client =
            Barrier.use daemon.distribution_barrier (handle_cmd client cmd)
          in
          (handle [@tailcall]) client
      in
      handle client
    and finally () =
      debug [ Pp.textf "%s: cleanup" (peer_name client.peer) ];
      let open LwtO in
      let+ () = close_client_socket client.fd in
      let () = Cache.Local.teardown client.cache in
      daemon.event_push (Some (Client_left client.fd))
    in
    try%lwt protect ~f ~finally with
    | Unix.Unix_error (Unix.EBADF, _, _) ->
      info [ Pp.textf "%s: ended" (peer_name client.peer) ];
      Lwt_result.return client
    | exn ->
      info
        [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
            (Printexc.to_string exn)
        ; Pp.text (Printexc.get_backtrace ())
        ];
      Lwt_result.return client
  with Code_error.E e as exn ->
    info
      [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
          (Dyn.to_string (Code_error.to_dyn e))
      ];
    raise exn

let run ?(endpoint_f = ignore) ?endpoint ?(trim_period = 10 * 60)
    ?(trim_size = 10_000_000_000L) daemon =
  let trim_thread max_size period cache =
    let period = float_of_int period in
    let open LwtO in
    let rec trim () =
      let* () = Lwt_unix.sleep period in
      let () =
        match
          let size = Cache.Local.overhead_size cache in
          if size > max_size then (
            info [ Pp.textf "trimming %Li bytes" (Int64.sub size max_size) ];
            Some (Cache.Local.trim cache ~goal:(Int64.sub size max_size))
          ) else
            None
        with
        | Some { trimmed_bytes = freed; _ } ->
          info [ Pp.textf "trimming freed %Li bytes" freed ]
        | None -> debug [ Pp.textf "skip trimming" ]
      in
      trim ()
    in
    trim ()
  in
  let rec accept_thread sock =
    let rec accept () =
      try%lwt Lwt_unix.accept sock
      with Unix.Unix_error (Unix.EINTR, _, _) -> (accept [@tailcall]) ()
    in
    let open LwtO in
    let* fd, peer = accept () in
    daemon.event_push (Some (New_client (fd, peer)));
    (accept_thread [@tailcall]) sock
  in
  let open LwtO in
  let endpoint =
    Option.value
      ~default:
        (Unix.ADDR_UNIX (default_runtime_file "socket" |> Path.to_string))
      endpoint
  in
  let sock =
    match endpoint with
    | Unix.ADDR_UNIX path ->
      let () =
        try Unix.unlink path with Unix.Unix_error (Unix.ENOENT, _, _) -> ()
      in
      Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0
    | Unix.ADDR_INET _ -> Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0
  in
  let () = daemon.socket <- Some sock in
  let* () = Lwt_unix.bind sock endpoint in
  let endpoint = getsockname (Lwt_unix.getsockname sock) in
  daemon.endpoint <- Some endpoint;
  endpoint_f (Uri.to_string endpoint);
  Lwt_unix.listen sock 1024;
  daemon.accept_thread <- Some (accept_thread sock);
  daemon.trim_thread <- Some (trim_thread trim_size trim_period daemon.cache);
  let rec handle () =
    let stop () =
      info [ Pp.textf "stop" ];
      let+ () =
        match daemon.socket with
        | Some fd -> (
          let sockname = Lwt_unix.getsockname fd in
          daemon.socket <- None;
          let clean f = Lwt_list.iter_p f (Clients.to_list daemon.clients) in
          let* () =
            clean (fun (fd, _) ->
                Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL |> Lwt.return)
          in
          let* () = clean (fun (_, (_, thread)) -> thread) in
          let* () = clean (fun (fd, _) -> Lwt_unix.close fd) in
          let* () = Lwt_unix.close fd in
          match sockname with
          | Unix.ADDR_UNIX path -> (
            try Lwt.return @@ Unix.unlink path
            with Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return () )
          | _ -> Lwt.return () )
        | None -> Lwt.return ()
      in
      Cache.Local.teardown daemon.cache
    in
    Lwt_stream.get daemon.events >>= function
    | None
    | Some Stop ->
      stop ()
    | Some (New_client (fd, peer)) ->
      let input = Lwt_io.of_fd ~mode:Lwt_io.input fd |> Csexp.Lwt.Input.make
      and output = Lwt_io.of_fd ~mode:Lwt_io.output fd in
      let* res =
        info [ Pp.textf "accept new client %s" (peer_name peer) ];
        let open LwtR in
        let* () =
          let version_message =
            Lang my_versions
            |> Cache.Messages.sexp_of_message { major = 1; minor = 0 }
          in
          Lwt_io.write output (Csexp.to_string version_message)
          |> Lwt.map Result.return
        in
        let* their_versions =
          let* msg = Csexp.Lwt.Parser.parse input in
          initial_message_of_sexp msg
          |> Result.map ~f:(fun (Lang res) -> res)
          |> Result.map_error ~f:(fun e -> `Protocol_error e)
          |> Lwt.return
        in
        let* version =
          find_newest_common_version my_versions their_versions
          |> Result.map_error ~f:(fun _ ->
                 `Version_mismatch (my_versions, their_versions))
          |> Lwt.return
        in
        debug
          [ Pp.textf "negotiated protocol version %s"
            @@ Cache.Messages.string_of_version version
          ];
        let client =
          let cache =
            match
              Cache.Local.make ~root:daemon.root
                ~duplication_mode:Cache.Duplication_mode.Hardlink
                ~command_handler:(client_handle peer version output)
                ()
            with
            | Result.Ok m -> m
            | Result.Error e -> User_error.raise [ Pp.textf "%s" e ]
          in
          { cache
          ; commits = [||]
          ; common_metadata = []
          ; fd
          ; input
          ; output
          ; peer
          ; repositories = []
          ; version
          }
        in
        let thread =
          let open Lwt.Infix in
          client_thread daemon client >|= function
          | Result.Ok _ -> ()
          | Result.Error (`Distribution_error e)
          | Result.Error (`Remote_cache_error e)
          | Result.Error (`Local_cache_error e)
          | Result.Error (`Parse_error e)
          | Result.Error (`Protocol_error e)
          | Result.Error (`Read_error e) ->
            info [ Pp.textf "%s: client error: %s" (peer_name peer) e ]
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
            info [ Pp.textf "reject client: %s" msg ]
          | `Version_mismatch _ ->
            info [ Pp.textf "reject client: version mismatch" ] );
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

let daemon ~root ?distribution ~config started =
  let dune_cache_daemon =
    let open LwtR in
    Path.mkdir_p root;
    let* daemon = make ~root ?distribution ~config () in
    (* Event blocks signals when waiting. Use a separate thread to catch
       signals. *)
    let signal_handler s =
      info [ Pp.textf "caught signal %i, exiting" (-s) ];
      daemon.event_push (Some Stop)
    and signals = [ Sys.sigint; Sys.sigterm ] in
    let _ =
      let f s = Lwt_unix.on_signal s signal_handler in
      List.map ~f signals
    in
    let ran = run ~endpoint_f:started daemon in
    Lwt.map Result.ok ran
  in
  match Lwt_main.run dune_cache_daemon with
  | Result.Ok () -> ()
  | Result.Error (`Fatal e)
  | Result.Error (`Local_cache_error e) ->
    Printf.fprintf stderr "%s: fatal error: %s\n%!" Sys.argv.(0) e;
    exit 1
