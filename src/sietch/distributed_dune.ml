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

and client = Async.Rpc.Connection.t

let make cache config = { cache; clients = Clients.empty; config }

let debug = Dune_util.Log.info

let digest_from_hex_exn h =
  match Digest.from_hex h with
  | None -> failwith "invalid hash"
  | Some h -> h

let connect t uri =
  let () =
    debug [ Pp.textf "connect to distributed backend %S" (Uri.to_string uri) ]
  in
  let* _, reader, writer =
    Async.Tcp.connect @@ Async.Tcp.Where_to_connect.of_host_and_port
    @@ Core.Host_and_port.of_string @@ Uri.to_string uri
    |> async_ok
  in
  let* client =
    let implementations =
      let get () h =
        let hash = digest_from_hex_exn h in
        let path = Local.file_path t.cache hash |> Path.to_string in
        let f () =
          let ( let+ ) = Async.( >>| ) in
          let ( and+ ) = Async.Deferred.both in
          let+ stats = Async.Unix.stat path
          and+ contents = Async.Reader.open_file path in
          Dune_distributed_storage.Rpc.encode_block_get
            (stats.perm land 0o100 <> 0)
            (Async.Reader.pipe contents)
        in
        Async.Deferred.map (Async.try_with f)
          ~f:(Result.map_error ~f:(fun _ -> ()))
      in
      match
        Async.Rpc.Implementations.create
          ~implementations:
            [ Async.Rpc.Pipe_rpc.implement
                Dune_distributed_storage.Rpc.block_get get
            ]
          ~on_unknown_rpc:`Raise
      with
      | Result.Ok impls -> impls
      | Result.Error (`Duplicate_implementations _) ->
        failwith "duplicate implementations"
    in
    Async.( >>| )
      (Async.Rpc.Connection.create
         ~connection_state:(fun _ -> ())
         ~implementations reader writer)
      (Result.map_error ~f:Core.Exn.to_string)
  in
  let* clients =
    Clients.add t.clients uri client
    |> Result.map_error ~f:(fun _ -> "duplicate client")
    (* FIXME: race condition *)
    |> Async.return
  in
  let () = t.clients <- clients in
  Async.Deferred.Result.return client

let client_and_uri t target =
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
  let+ client =
    match Clients.find t.clients uri with
    | Some client -> Async.Deferred.Result.return client
    | None -> connect t uri
  in
  (client, uri)

let client t target = client_and_uri t target >>| fst

let block_get t hash =
  let* client = client t hash in
  let ( >>= ) = Async.( >>= ) in
  Async.Rpc.Pipe_rpc.dispatch Dune_distributed_storage.Rpc.block_get client
    (Digest.to_string hash)
  >>= Dune_distributed_storage.Rpc.decode_block_get |> async_ok

let _block_has t hash =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.block_has client
    (Digest.to_string hash)
  |> async_ok

let _block_put t hash executable contents =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.block_put client
    (Digest.to_string hash, executable, contents)
  |> async_ok

let index_get t name hash =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.index_get client
    (name, Digest.to_string hash)
  |> async_ok

let index_put t name hash lines =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.index_put client
    (name, Digest.to_string hash, lines)
  |> async_ok

let metadata_put t hash (metadata : Cache.Local.Metadata_file.t) =
  let* targets =
    let ( let* ) = Async.( >>= ) in
    let* init =
      let+ client, uri = client_and_uri t hash in
      Clients.set Clients.empty uri client
    in
    let f init { Cache.File.digest; _ } =
      match init with
      | Result.Ok init ->
        let+ client, uri = client_and_uri t digest in
        Clients.set init uri client
      | Result.Error e -> Async.Deferred.Result.fail e
    in
    match metadata.contents with
    | Value v ->
      let+ client, uri = client_and_uri t v in
      Clients.set Clients.empty uri client
    | Files files -> Async.Deferred.List.fold ~f ~init files
  in
  let f (_, client) =
    Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.metadata_put client
      (Digest.to_string hash, Cache.Local.Metadata_file.to_string metadata)
  in
  Async.Deferred.List.iter ~f (Clients.to_list targets) |> async_ok

let distribute t key (metadata : Cache.Local.Metadata_file.t) =
  let f () = metadata_put t key metadata in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "distribute fatal error" [ ("exception", Exn.to_dyn e) ]

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
      let () = debug [ Pp.textf "fetch metadata %s (%i/%i)" hash i count ] in
      block_get t key >>= function
      | None -> Async.Deferred.Result.return ()
      | Some (metadata_contents, _) -> (
        let* metadata_contents =
          Async.Pipe.fold ~init:""
            ~f:(fun l r -> Async.return (l ^ r))
            metadata_contents
          |> async_ok
        in
        let* metadata =
          Cache.Local.Metadata_file.of_string metadata_contents |> Async.return
        in
        match metadata.contents with
        | Files files ->
          let fetch { Cache.File.digest; _ } =
            let path = Local.file_path cache digest in
            if Path.exists local_path then
              debug
                [ Pp.textf "artifact already present locally: %s"
                    (Path.to_string local_path)
                ]
              |> Async.Deferred.Result.return
            else
              let () =
                debug [ Pp.textf "fetch artifact %s" (Digest.to_string digest) ]
              in
              block_get t digest >>= function
              | None -> Async.Deferred.Result.return ()
              | Some (contents, executable) ->
                let f writer =
                  Async.Pipe.transfer ~f:Core.Fn.id contents
                    (Async.Writer.pipe writer)
                in
                write_file cache path executable ~f
          in
          let ( let* ) = Async.Deferred.( >>= ) in
          let* results = Async.Deferred.List.all @@ List.map ~f:fetch files in
          let ( let* ) = Async.Deferred.Result.( >>= ) in
          let* (_ : unit list) = results |> Result.List.all |> Async.return in
          write_file cache local_path false ~f:(fun writer ->
              Async.return @@ Async.Writer.write writer metadata_contents)
        | Value h ->
          let () =
            debug [ Pp.textf "skipping Jenga value: %s" (Digest.to_string h) ]
          in
          Async.Deferred.Result.return () )
  in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "prefetch fatal error" [ ("exception", Exn.to_dyn e) ]

let index_add t name key keys =
  let keys = List.map ~f:Digest.to_string keys in
  let f () = index_put t name key keys in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok v -> v
  | Result.Error e ->
    Code_error.raise "index_add fatal error" [ ("exception", Exn.to_dyn e) ]

let index_prefetch t name key =
  let f () =
    index_get t name key >>= function
    | Some lines ->
      let () =
        debug [ Pp.textf "prefetch up to %d artifacts" (List.length lines) ]
      in
      let keys = List.filter_map ~f:(fun d -> Digest.from_hex d) lines in
      let count = List.length keys in
      let ( let* ) = Async.Deferred.( >>= ) in
      let* results =
        Async.Deferred.List.mapi ~how:(`Max_concurrent_jobs 8)
          ~f:(prefetch t count) keys
      in
      let ( let* ) = Async.Deferred.Result.( >>= ) in
      let* (_ : unit list) = Result.List.all results |> Async.return in
      Async.Deferred.Result.return ()
    | None -> Async.Deferred.Result.return ()
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
