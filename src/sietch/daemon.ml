module Evt = Event
module Utils = Utils
module Log = Dune_util.Log
open Stdune
open Utils
open Cache.Messages
open Pp.O

type 'a socket = ('a, Async.Socket.Address.Unix.t) Async.Socket.t

type error =
  [ `Local_cache_error of string
  | `Fatal of string
  ]

type client =
  { cache : Cache.Local.t
  ; commits : Digest.t list array
  ; common_metadata : Sexp.t list
  ; socket : [ `Active ] socket
  ; input : Async.Reader.t
  ; output : Async.Writer.t
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
  let open Result.O in
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

let write output s =
  let () = Async.Writer.write output s in
  Async.Writer.flushed output |> Async.Deferred.map ~f:Result.return

let send_sexp output sexp =
  let f () = write output @@ Csexp.to_string sexp in
  let ( >>= ) = Async.( >>= ) in
  Async.try_with ~extract_exn:true f >>= function
  | Result.Ok v -> Async.Deferred.return v
  | Result.Error (Unix.Unix_error (e, f, _)) ->
    Async.Deferred.Result.fail
      (`Write_error (Printf.sprintf "%s: %s" (Unix.error_message e) f))
  | Result.Error e -> raise e

let send version output message =
  send_sexp output (sexp_of_message version message)

module ClientsKey = struct
  type t = [ `Active ] socket

  let compare a b = Ordering.of_int (Stdlib.compare a b)

  let to_dyn _ = Dyn.Opaque
end

module Clients = Map.Make (ClientsKey)

type config = { exit_no_client : bool }

type event =
  | Stop
  | New_client of [ `Active ] socket * Async.Socket.Address.Unix.t
  | Client_left of [ `Active ] socket

type t =
  { root : Path.t
  ; mutable socket : [ `Passive ] socket option
  ; mutable clients : (client * unit Async.Deferred.t) Clients.t
  ; mutable endpoint : Uri.t option
  ; mutable accept_thread : unit Async.Deferred.t option
  ; mutable trim_thread : unit Async.Deferred.t option
  ; config : config
  ; events : event Async.Pipe.Reader.t
  ; push_event : event Async.Pipe.Writer.t
  ; cache : Cache.Local.t
  ; distributed : (module Distributed.S)
  ; distribution_pipe :
      (Cache.Key.t * Cache.Local.Metadata_file.t) Async.Pipe.Writer.t
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
  | Result.Error msg -> Async.Deferred.Result.fail (`Local_cache_error msg)
  | Result.Ok cache ->
    let events, push_event = Async.Pipe.create ()
    and distributed = distribution cache in
    let distribution_pipe =
      let rec f reader =
        let open Async in
        Async.Pipe.read reader >>= function
        | `Ok (key, metadata) ->
          let module D = (val distributed) in
          let open Async in
          D.distribute key metadata >>= fun res ->
          log_error (Digest.to_string key) "distribute" res;
          f reader
        | `Eof -> return ()
      in
      Async.Pipe.create_writer f
    in
    Async.Deferred.Result.return
      { root = Option.value root ~default:(Cache.Local.default_root ())
      ; socket = None
      ; clients = Clients.empty
      ; endpoint = None
      ; accept_thread = None
      ; trim_thread = None
      ; config
      ; events
      ; push_event
      ; cache
      ; distributed
      ; distribution_pipe
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
      Async.Deferred.map ~f sent
    in
    (* Because the Cache.Local expect a -> unit handler, we have to run this
       asynchronously *)
    (* FIXME: Hence we should probably lock. *)
    ignore send

let protect ~f ~finally =
  let open Async in
  try_with ~extract_exn:true f >>= function
  | Result.Ok v -> finally () >>| fun () -> v
  | Result.Error e -> finally () >>| fun () -> raise e

let outgoing_message_of_sexp version sexp =
  outgoing_message_of_sexp version sexp
  |> Result.map_error ~f:(fun s -> `Protocol_error s)

let close_client_socket socket =
  let () =
    try Async.Socket.shutdown socket `Both
    with Unix.Unix_error (Unix.ENOTCONN, _, _) -> ()
  in
  Async.Unix.close (Async.Socket.fd socket)

let commits_index_key = "commits"

let index_commits { distributed = (module Distributed); _ }
    ({ commits; repositories; _ } as client) =
  let open Async in
  let* results =
    let f i (repository : Cache.repository) =
      let commits = commits.(i)
      and key = Digest.string repository.commit in
      Distributed.index_add commits_index_key key commits
    in
    Async.Deferred.List.mapi ~f repositories >>| Result.return
  in
  let+ () =
    Result.List.all results
    |> Result.map ~f:(fun (_ : unit list) -> ())
    |> Result.map_error ~f:(fun e -> `Distribution_error e)
    |> Async.return
  in
  { client with commits = [||]; repositories = [] }

let client_thread daemon client =
  try
    let handle_cmd (client : client) sexp =
      let* msg = outgoing_message_of_sexp client.version sexp |> Async.return in
      let promoted metadata { repository; key; _ } =
        let () =
          Async.Pipe.write_without_pushback daemon.distribution_pipe
            (key, metadata)
        in
        let register_commit repository =
          client.commits.(repository) <- key :: client.commits.(repository)
        in
        Option.iter ~f:register_commit repository
      in
      match msg with
      | Hint keys ->
        let module D = (val daemon.distributed) in
        let f k = D.prefetch k in
        let _prefetching =
          let open Async in
          Async.Deferred.List.map ~f keys
          >>| List.iter ~f:(log_error (peer_name client.peer) "prefetching")
        in
        Async.Deferred.Result.return client
      | Promote ({ duplication; repository; files; key; metadata } as promotion)
        ->
        let metadata = metadata @ client.common_metadata in
        let* metadata, _ =
          Cache.Local.promote_sync client.cache files key metadata ~repository
            ~duplication
          |> Result.map_error ~f:(fun e -> `Distribution_error e)
          |> Async.return
        in
        let () = promoted metadata promotion in
        Async.Deferred.Result.return client
      | Promoted ({ key; _ } as promotion) ->
        let* metadata =
          let path = Cache.Local.metadata_path client.cache key in
          let* sexp =
            try
              Io.read_file path |> Csexp.parse_string
              |> Result.map_error ~f:(fun (_, e) -> `Parse_error e)
              |> Async.return
            with Sys_error e ->
              let path = Path.to_string path in
              let msg = sprintf "error while reading metadata file %s: %s" path e in
              Async.Deferred.Result.fail
              @@ `Read_error msg
          in
          Cache.Local.Metadata_file.of_sexp sexp
          |> Result.map_error ~f:(fun e -> `Parse_error e)
          |> Async.return
        in
        let () = promoted metadata promotion in
        Async.Deferred.Result.return client
      | SetBuildRoot root ->
        let res =
          let open Result.O in
          let+ cache = Cache.Local.set_build_dir client.cache root in
          { client with cache }
        in
        res
        |> Result.map_error ~f:(fun e -> `Local_cache_error e)
        |> Async.return
      | SetCommonMetadata metadata ->
        Async.Deferred.Result.return { client with common_metadata = metadata }
      | SetRepos repositories ->
        let _prefetching =
          let module D = (val daemon.distributed) in
          let f (repository : Cache.repository) =
            info [ Pp.textf "prefetch commit %S" repository.commit ];
            D.index_prefetch commits_index_key (Digest.string repository.commit)
          in
          let open Async in
          Async.Deferred.List.map ~f repositories >>= fun results ->
          match Result.List.all results with
          | Result.Ok (_ : unit list) -> Async.return ()
          | Result.Error e ->
            info
              [ Pp.textf "error while prefetching index %s: %s"
                  commits_index_key e
              ];
            Async.return ()
        in
        let* client = index_commits daemon client in
        let* cache =
          Cache.Local.with_repositories client.cache repositories
          |> Result.map_error ~f:(fun s -> `Remote_cache_error s)
          |> Async.return
        in
        let commits = Array.make (List.length repositories) [] in
        Async.Deferred.Result.return
          { client with cache; commits; repositories }
    in
    let input = client.input in
    let f () =
      let rec handle client =
        let open Async.Deferred.Result in
        debug [ Pp.textf "%s: read next command" (peer_name client.peer) ];
        read_char input >>= function
        | None ->
          debug [ Pp.textf "%s: ended" (peer_name client.peer) ];
          let* client = index_commits daemon client in
          Async.Deferred.Result.return client
        | Some '\n' ->
          (* Skip toplevel newlines, for easy netcat interaction *)
          (handle [@tailcall]) client
        | Some c ->
          let* cmd = parse_sexp ~c input in
          debug
            [ Pp.box ~indent:2
              @@ Pp.textf "%s: received command" (peer_name client.peer)
                 ++ Pp.space
                 ++ Pp.hbox (Pp.text @@ Stdune.Sexp.to_string cmd)
            ];
          let* client = handle_cmd client cmd in
          (handle [@tailcall]) client
      in
      handle client
    and finally () =
      debug [ Pp.textf "%s: cleanup" (peer_name client.peer) ];
      let ( let* ) = Async.Deferred.( >>= ) in
      let* () = close_client_socket client.socket in
      let () = Cache.Local.teardown client.cache in
      Async.Pipe.write daemon.push_event (Client_left client.socket)
    in
    let ( >>| ) = Async.( >>| ) in
    Async.try_with ~extract_exn:true (fun () -> protect ~f ~finally)
    >>| function
    | Result.Ok v -> v
    | Result.Error (Unix.Unix_error (Unix.EBADF, _, _)) ->
      let () = info [ Pp.textf "%s: ended" (peer_name client.peer) ] in
      Result.return client
    | Result.Error exn ->
      let () =
        info
          [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
              (Printexc.to_string exn)
          ; Pp.text (Printexc.get_backtrace ())
          ]
      in
      Result.return client
  with Code_error.E e as exn ->
    info
      [ Pp.textf "%s: fatal error: %s" (peer_name client.peer)
          (Dyn.to_string (Code_error.to_dyn e))
      ];
    raise exn

let run ?(endpoint_f = ignore) ?endpoint ?(trim_period = 10 * 60)
    ?(trim_size = 10_000_000_000L) daemon =
  let trim_thread max_size period cache =
    let ( let* ) = Async.( >>= ) in
    let period = float_of_int period in
    let rec trim () =
      let* () = Async.Clock.after (Core.Time.Span.of_sec period) in
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
    let ( >>= ) = Async.( >>= ) in
    Async.Socket.accept sock >>= function
    | `Ok (socket, peer) ->
      let () =
        Async.Pipe.write daemon.push_event (New_client (socket, peer)) |> ignore
      in
      (accept_thread [@tailcall]) sock
    | `Socket_closed -> Async.return ()
  in
  let endpoint =
    Option.value
      ~default:
        (Unix.ADDR_UNIX (default_runtime_file "socket" |> Path.to_string))
      endpoint
  in
  let* sock =
    let open Async.Socket in
    match endpoint with
    | Unix.ADDR_UNIX path ->
      let () =
        try Unix.unlink path with Unix.Unix_error (Unix.ENOENT, _, _) -> ()
      in
      let socket = create Type.unix in
      let+ bound =
        bind socket (`Unix path) |> Async.Deferred.map ~f:Result.return
      in
      listen bound
    | Unix.ADDR_INET _ -> failwith "TCP support dropped for now"
    (* let socket = create Type.tcp in
     * let+ bound =
     *   bind socket (`Inet (addr, port)) |> Async.Deferred.map ~f:Result.return
     * in
     * bound *)
  in
  let () = daemon.socket <- Some sock in
  let endpoint =
    getsockname Async.Socket.(getsockname sock |> Address.to_sockaddr)
  in
  daemon.endpoint <- Some endpoint;
  endpoint_f (Uri.to_string endpoint);
  daemon.accept_thread <- Some (accept_thread sock);
  daemon.trim_thread <- Some (trim_thread trim_size trim_period daemon.cache);
  let rec handle () =
    let stop () =
      info [ Pp.textf "stop" ];
      let+ () =
        match daemon.socket with
        | Some socket -> (
          let sockname =
            (Async.Socket.getsockname socket :> Async.Socket.Address.t)
          in
          daemon.socket <- None;
          let clean f =
            Async.Deferred.List.iter ~f (Clients.to_list daemon.clients)
          in
          let ( let* ) = Async.( >>= ) in
          let* () =
            clean (fun (socket, _) ->
                Async.Socket.shutdown socket `Both |> Async.return)
          in
          let* () = clean (fun (_, (_, thread)) -> thread) in
          let* () =
            clean (fun (socket, _) -> Async.Unix.close (Async.Socket.fd socket))
          in
          let* () = Async.Unix.close (Async.Socket.fd socket) in
          match sockname with
          | `Unix path -> (
            try Async.Deferred.Result.return @@ Unix.unlink path
            with Unix.Unix_error (Unix.ENOENT, _, _) ->
              Async.Deferred.Result.return () )
          | _ -> Async.Deferred.Result.return () )
        | None -> Async.Deferred.Result.return ()
      in
      Cache.Local.teardown daemon.cache
    in
    let ( >>= ) = Async.Deferred.( >>= ) in
    Async.Pipe.read daemon.events >>= function
    | `Eof
    | `Ok Stop ->
      stop ()
    | `Ok (New_client (socket, peer)) ->
      let fd = Async.Socket.fd socket in
      let input = Async.Reader.create fd
      and output = Async.Writer.create fd in
      let res =
        info
          [ Pp.textf "accept new client %s"
              (peer_name (Async.Socket.Address.to_sockaddr peer))
          ];
        let* () =
          let version_message =
            Lang my_versions
            |> Cache.Messages.sexp_of_message { major = 1; minor = 0 }
          in
          write output (Csexp.to_string version_message)
        in
        let* their_versions =
          let* msg = parse_sexp input in
          initial_message_of_sexp msg
          |> Result.map ~f:(fun (Lang res) -> res)
          |> Result.map_error ~f:(fun e -> `Protocol_error e)
          |> Async.return
        in
        let* version =
          find_newest_common_version my_versions their_versions
          |> Result.map_error ~f:(fun _ ->
                 `Version_mismatch (my_versions, their_versions))
          |> Async.return
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
                ~command_handler:
                  (client_handle
                     (Async.Socket.Address.to_sockaddr peer)
                     version output)
                ()
            with
            | Result.Ok m -> m
            | Result.Error e -> User_error.raise [ Pp.textf "%s" e ]
          in
          { cache
          ; commits = [||]
          ; common_metadata = []
          ; socket
          ; input
          ; output
          ; peer = Async.Socket.Address.to_sockaddr peer
          ; repositories = []
          ; version
          }
        in
        let thread =
          let ( >>| ) = Async.Deferred.( >>| ) in
          client_thread daemon client >>| function
          | Result.Ok _ -> ()
          | Result.Error (`Distribution_error e)
          | Result.Error (`Remote_cache_error e)
          | Result.Error (`Local_cache_error e)
          | Result.Error (`Parse_error e)
          | Result.Error (`Protocol_error e)
          | Result.Error (`Read_error e) ->
            info
              [ Pp.textf "%s: client error: %s"
                  (peer_name (Async.Socket.Address.to_sockaddr peer))
                  e
              ]
        in
        match Clients.add daemon.clients client.socket (client, thread) with
        | Result.Ok clients ->
          daemon.clients <- clients;
          Async.Deferred.Result.return ()
        | Result.Error _ -> Code_error.raise "duplicate socket" []
      in
      let* () =
        let ( >>= ) = Async.Deferred.( >>= ) in
        res >>= function
        | Result.Ok () -> Async.Deferred.Result.return ()
        | Result.Error e ->
          ( match e with
          | `Parse_error msg
          | `Protocol_error msg
          | `Read_error msg
          | `Write_error msg ->
            info [ Pp.textf "reject client: %s" msg ]
          | `Version_mismatch _ ->
            info [ Pp.textf "reject client: version mismatch" ] );
          close_client_socket socket |> Async.Deferred.map ~f:Result.return
      in
      (handle [@tailcall]) ()
    | `Ok (Client_left socket) ->
      daemon.clients <- Clients.remove daemon.clients socket;
      let* () =
        if daemon.config.exit_no_client && Clients.is_empty daemon.clients then
          stop ()
        else
          Async.Deferred.Result.return ()
      in
      if Option.is_some daemon.socket then
        (handle [@tailcall]) ()
      else
        Async.Deferred.Result.return ()
  in
  handle ()

let () = Logs.set_reporter (Logs.format_reporter ())

let daemon ~root ?distribution ~config started =
  let dune_cache_daemon () =
    Path.mkdir_p root;
    let* daemon = make ~root ?distribution ~config () in
    let f s =
      info [ Pp.textf "caught signal %s, exiting" (Async.Signal.to_string s) ];
      Async.Pipe.write daemon.push_event Stop |> ignore
    and signals = [ Async.Signal.int; Async.Signal.term ] in
    let () = Async.Signal.handle signals ~f in
    run ~endpoint_f:started daemon
  in
  match Async.Thread_safe.block_on_async_exn dune_cache_daemon with
  | Result.Ok () -> ()
  | Result.Error (`Fatal e)
  | Result.Error (`Local_cache_error e) ->
    Printf.fprintf stderr "%s: fatal error: %s\n%!" Sys.argv.(0) e;
    exit 1
