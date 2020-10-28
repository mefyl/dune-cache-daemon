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
    Async.( >>| )
      (Async.Rpc.Connection.create
         ~connection_state:(fun _ -> ())
         reader writer)
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

let block_get t hash =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.block_get client
    (Digest.to_string hash)
  |> async_ok

let block_has t hash =
  let* client = client t hash in
  Async.Rpc.Rpc.dispatch_exn Dune_distributed_storage.Rpc.block_has client
    (Digest.to_string hash)
  |> async_ok

let block_put t hash executable contents =
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

let distribute ({ cache; _ } as t) key (metadata : Cache.Local.Metadata_file.t)
    =
  let f () =
    let* () =
      match metadata.contents with
      | Files files ->
        let insert_file { Cache.File.digest; _ } =
          let path = Local.file_path cache digest in
          let insert stats input =
            let executable = stats.Unix.st_perm land 0o100 > 0 in
            let* contents = Async.Reader.contents input |> async_ok in
            block_put t digest executable contents
          in
          let stats = Path.stat path in
          let* skip =
            if stats.st_size < 4096 then
              Async.Deferred.Result.return false
            else
              block_has t digest
          in
          if skip then
            Async.Deferred.Result.return ()
          else
            let path = Path.to_string path in
            let () = debug [ Pp.textf "distribute %S" path ] in
            Async.Reader.with_file path ~f:(insert stats)
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
    block_put t key false (Cache.Local.Metadata_file.to_string metadata)
  in
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
                let perm =
                  if executable then
                    0o500
                  else
                    0o400
                in
                Async.Writer.with_file_atomic ~perm (Path.to_string path)
                  ~f:(fun writer ->
                    let () = Async.Writer.write writer contents in
                    Async.Writer.flushed writer)
                |> async_ok
          in
          let ( let* ) = Async.Deferred.( >>= ) in
          let* results = Async.Deferred.List.all @@ List.map ~f:fetch files in
          let ( let* ) = Async.Deferred.Result.( >>= ) in
          let* (_ : unit list) = results |> Result.List.all |> Async.return in
          write_file cache local_path false metadata_contents
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
