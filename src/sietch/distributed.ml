open Stdune
open Cache
open Utils
include Distributed_intf
module Metadata_file = Cache.Local.Metadata_file
module Log = Dune_util.Log

let ( let* ) = Lwt.Infix.( >>= )

let ( and* ) = Lwt.both

let disabled =
  ( module struct
    type t = unit

    let v = ()

    let distribute _ _ = Lwt_result.return ()

    let prefetch _ = Lwt_result.return ()

    let index_add _ _ _ = Lwt_result.return ()

    let index_prefetch _ _ = Lwt_result.return ()
  end : S )

let _irmin (type t) cache
    (module Store : Irmin.S
      with type t = t
       and type step = string
       and type key = string list
       and type contents = string
       and type metadata = bool) (store : t Lwt.t) =
  ( module struct
    include Store

    let convert_irmin_error =
      let f = function
        | `Conflict reason -> Format.sprintf "Irmin conflict: %s" reason
        | `Test_was _ -> Format.sprintf "Irmin test_was"
        | `Too_many_retries i -> Format.sprintf "Irmin too many retries: %d" i
      in
      Lwt.map (Result.map_error ~f)

    let compare_trees l r = Store.Tree.hash l = Store.Tree.hash r

    type nonrec t =
      { fd_pool : unit Lwt_pool.t
      ; insert_mutex : Lwt_mutex.t
      ; store : t
      }

    let v =
      { fd_pool = Lwt_pool.create 128 (fun () -> Lwt.return ())
      ; insert_mutex = Lwt_mutex.create ()
      ; store = Lwt_main.run store
      }

    let tmp = Cache.Local.path_tmp cache "sietch"

    let find_or_create_tree tree path =
      Store.Tree.find_tree tree path
      |> Lwt.map (Option.value ~default:Store.Tree.empty)

    let distribute key (metadata : Metadata_file.t) =
      let open LwtO in
      let insert (root : Store.tree option) =
        let root =
          match root with
          | None -> Store.Tree.empty
          | Some tree -> tree
        and insert_file (tree, count)
            { File.digest; in_the_cache; in_the_build_directory } =
          let stats = Path.stat in_the_cache in
          let checksum =
            Stdune.Digest.file_with_stats in_the_cache stats
            |> Stdune.Digest.to_string
          and basename = Path.basename in_the_cache in
          if checksum ^ ".1" <> basename then
            Stdune.User_warning.emit
              [ Pp.textf "checksum mismatch for %s: %s <> %s"
                  (Path.Local.to_string
                     (Path.Build.local in_the_build_directory))
                  checksum basename
              ];
          let* contents =
            let read () =
              let* file =
                Lwt_io.open_file ~mode:Lwt_io.input
                  (Path.to_string in_the_cache)
              in
              let* contents = Lwt_io.read file in
              let+ () = Lwt_io.close file in
              contents
            in
            Lwt_pool.use v.fd_pool read
          in
          let metadata = stats.st_perm land 0o100 != 0 in
          let+ new_tree =
            Store.Tree.add tree ~metadata [ Digest.to_string digest ] contents
          in
          let (_ : hash) = Store.Tree.hash new_tree in
          if compare_trees new_tree tree then
            (new_tree, count)
          else (
            Log.info
              [ Pp.textf "distribute data file %s" (Digest.to_string digest) ];
            (new_tree, count + 1)
          )
        in
        let* tree_files, count =
          let* tree_files = find_or_create_tree root [ "files" ] in
          Lwt_list.fold_left_s insert_file (tree_files, 0) metadata.files
        in
        let* tree_metadata, meta_changed =
          let* tree_metadata = find_or_create_tree root [ "meta" ] in
          let+ new_tree =
            Store.Tree.add tree_metadata [ Digest.to_string key ]
              (Metadata_file.to_string metadata)
          in
          if compare_trees new_tree tree_metadata then
            (new_tree, false)
          else (
            Log.info
              [ Pp.textf "distribute metadata file %s" (Digest.to_string key) ];
            (new_tree, true)
          )
        in
        if meta_changed || count > 0 then
          let* root = Store.Tree.add_tree root [ "files" ] tree_files in
          let+ root = Store.Tree.add_tree root [ "meta" ] tree_metadata in
          Some root
        else
          Lwt.return (Some root)
      and info () =
        let author = "sietch <https://github.com/ocaml/dune>"
        and date = Int64.of_float (Unix.gettimeofday ())
        and message =
          Format.asprintf "Promotion of rule %s" (Digest.to_string key)
        in
        Irmin.Info.v ~author ~date message
      in
      let distribute () =
        Store.with_tree ~info v.store [] insert |> convert_irmin_error
      in
      Lwt_mutex.with_lock v.insert_mutex distribute

    let mkdir p =
      try%lwt Lwt_unix.mkdir p 0o700
      with Unix.Unix_error (Unix.EEXIST, _, _) -> Lwt.return ()

    let write_file path (contents, executable) =
      try%lwt
        Lwt.map Result.ok
        @@
        let dir = path |> Path.parent_exn |> Path.to_string
        and path_tmp =
          path |> Path.basename |> Path.relative tmp |> Path.to_string
        and path = path |> Path.to_string
        and perm =
          if executable then
            0o500
          else
            0o400
        in
        let* () =
          let write () =
            let* output = Lwt_io.open_file ~perm ~mode:Lwt_io.output path_tmp in
            let* () = Lwt_io.write output contents in
            Lwt_io.close output
          in
          Lwt_pool.use v.fd_pool write
        and* () = mkdir dir in
        Lwt_unix.rename path_tmp path
      with
      | Unix.Unix_error (Unix.EACCES, _, _) ->
        (* If the file exists with no write permissions, it is being pulled as
           part of another hinting. *)
        Lwt_result.return ()
      | Unix.Unix_error (e, f, a) ->
        Lwt_result.fail (Printf.sprintf "%s: %s %s" (Unix.error_message e) f a)

    let search_missing_file t ~of_path ~of_string path dir key =
      if Path.exists path then (
        Log.info
          [ Pp.textf "%s file already present locally: %s" t
              (Digest.to_string key)
          ];
        let f d = Some (d, None) in
        of_path path |> Result.map ~f |> Lwt.return
      ) else
        let open LwtO in
        Store.find_all v.store [ dir; Digest.to_string key ] >|= function
        | None ->
          Log.info
            [ Pp.textf "%s file not found in the distributed cache: %s" t
                (Digest.to_string key)
            ];
          Result.Ok None
        | Some (contents, metadata) ->
          Log.info [ Pp.textf "retrieved %s file %s" t (Digest.to_string key) ];
          let f d = Some (d, Some (contents, metadata)) in
          of_string contents |> Result.map ~f

    let rec prefetch key =
      let open LwtrO in
      let path = Cache.Local.metadata_path cache key in
      let* metadata =
        search_missing_file "metadata" ~of_path:Metadata_file.parse
          ~of_string:Metadata_file.of_string path "meta" key
      in
      match metadata with
      | None -> Result.Ok () |> Lwt.return
      | Some (metadata, contents) ->
        let* () = prefetch_data metadata in
        let* () =
          contents
          |> Option.map ~f:(write_file path)
          |> Option.value ~default:(Lwt_result.return ())
        in
        Lwt.return (Result.Ok ())

    and prefetch_data (metadata : Metadata_file.t) =
      let open LwtrO in
      let retrieve_file (f : File.t) =
        let path = Cache.Local.file_path cache f.digest in
        let path = Path.of_string (Path.to_string path ^ ".1") in
        let* contents =
          search_missing_file "data"
            ~of_path:(fun _ -> Result.Ok ())
            ~of_string:(fun _ -> Result.Ok ())
            path "files" f.digest
        in
        match contents with
        | Some ((), Some contents) -> (
          let+ () = write_file path contents in
          try
            let checksum =
              Stdune.Digest.file_with_stats path (Path.stat path)
            in
            if checksum <> f.digest then
              Stdune.User_warning.emit
                [ Pp.textf "checksum mismatch for %s: %s <> %s"
                    (Path.Local.to_string
                       (Path.Build.local f.in_the_build_directory))
                    (Digest.to_string checksum)
                    (Digest.to_string f.digest)
                ]
          with Unix.Unix_error (Unix.ENOENT, _, _) ->
            (* Can happen if multiple rules are fetching this file, this
               iteration will silently skip it *)
            () )
        | _ -> Lwt_result.return ()
      in
      let open LwtO in
      let+ results = Lwt_list.map_p retrieve_file metadata.files in
      Result.List.iter ~f:(fun r -> r) results

    module Digests = Set.Make (Digest) (Map.Make (Digest))

    let parse_index contents =
      match Csexp.parse_string contents with
      | Result.Ok (Sexp.List keys) ->
        let f = function
          | Sexp.Atom key -> (
            match Digest.from_hex key with
            | Some key -> Result.Ok key
            | None -> Result.Error (Format.sprintf "invalid key: %s" key) )
          | sexp ->
            Result.Error
              (Format.sprintf "invalid key: %s" (Sexp.to_string sexp))
        in
        Result.List.map ~f keys
      | _ -> Result.Error "parse error"

    let index_add name key keys =
      let open LwtO in
      let path = [ Digest.to_string key ] in
      let add index =
        let index =
          match index with
          | None -> Store.Tree.empty
          | Some tree -> tree
        in
        let* keys =
          let+ contents = Store.Tree.find index path in
          match contents with
          | Some contents -> (
            match parse_index contents with
            | Result.Ok l ->
              let previous = Digests.of_list l
              and keys = Digests.of_list keys in
              Digests.union previous keys |> Digests.to_list
            | Result.Error reason ->
              Stdune.User_warning.emit
                [ Pp.textf "dropping invalid index file in index %s for %s: %s"
                    name (Digest.to_string key) reason
                ];
              keys )
          | None -> keys
        in
        let contents =
          Csexp.to_string
            (Sexp.List
               (List.map ~f:(fun key -> Sexp.Atom (Digest.to_string key)) keys))
        in
        let+ index = Store.Tree.add index ~metadata:false path contents in
        Some index
      and info () =
        let author = "sietch <https://github.com/ocaml/dune>"
        and date = Int64.of_float (Unix.gettimeofday ())
        and message = Format.asprintf "Indexing of %s" (Digest.to_string key) in
        Irmin.Info.v ~author ~date message
      in
      Log.info
        [ Pp.textf "add %s index for %s: %d entries" name (Digest.to_string key)
            (List.length keys)
        ];
      Store.with_tree ~info v.store [ "indexes"; name ] add
      |> convert_irmin_error

    let index_prefetch name key =
      let open LwtO in
      Store.find_all v.store [ "indexes"; name; Digest.to_string key ]
      >>= function
      | None ->
        Log.info
          [ Pp.textf "%s not found in index %s" (Digest.to_string key) name ];
        Lwt_result.return ()
      | Some (contents, _) -> (
        match parse_index contents with
        | Result.Ok keys ->
          Log.info
            [ Pp.textf "retrieve %s from index %s: %d entries"
                (Digest.to_string key) name (List.length keys)
            ];
          Lwt_list.map_p prefetch keys
          |> Lwt.map (fun l -> Result.List.all l |> Result.map ~f:ignore)
        | Result.Error reason ->
          Stdune.User_warning.emit
            [ Pp.textf "ignoring invalid index file in index %s for %s: %s" name
                (Digest.to_string key) reason
            ];
          Lwt_result.return () )
  end : S )

module Metadata = struct
  type t = bool

  let t = Irmin.Type.bool

  let default = false

  let merge =
    Irmin.Merge.v t (fun ~old:_ l r ->
        if l = r then
          Irmin.Merge.ok l
        else
          Irmin.Merge.conflict "executable bit conflict")
end

let irmin local =
  let module Store =
    Irmin_mem.Make (Metadata) (Irmin.Contents.String) (Irmin.Path.String_list)
      (Irmin.Branch.String)
      (Irmin.Hash.BLAKE2B)
  in
  let store =
    let open Lwt.Infix in
    Store.Repo.v (Irmin_mem.config ()) >>= fun repo -> Store.master repo
  in
  _irmin local (module Store) store

(* module GitFS = struct
 *   include Git_unix.Store
 *
 *   let v ?dotgit ?compression ?buffers root =
 *     let buffer =
 *       match buffers with
 *       | None -> None
 *       | Some p -> Some (Lwt_pool.use p)
 *     in
 *     v ?dotgit ?compression ?buffer root
 * end
 *
 * let irmin_git local path =
 *   let module Store =
 *     Irmin_git.KV (GitFS) (Git_unix.Sync (GitFS)) (Irmin.Contents.String)
 *   in
 *   let store =
 *     let open Lwt.Infix in
 *     Store.Repo.v (Irmin_git.config ~bare:false path) >>= fun repo ->
 *     Store.master repo
 *   in
 *   _irmin local (module Store) store *)
