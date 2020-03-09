open Stdune
open Cache
include Distributed_intf
module Metadata_file = Cache.Local.Metadata_file
module Log = Dune_util.Log

module type Monad = sig
  type 'a t

  module Infix : sig
    val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t

    val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
  end
end

module Let_syntax (O : Monad) = struct
  include O.Infix

  let ( let* ) = ( >>= )

  let ( let+ ) = ( >|= )
end

module Lwt_result = struct
  include Lwt_result

  type error = string

  type nonrec 'a t = ('a, error) t
end

let disabled =
  ( module struct
    type t = unit

    let v = ()

    let distribute _ _ = Result.Ok ()

    let prefetch _ = Result.Ok ()
  end : S )

let _irmin (type t) cache
    (module Store : Irmin.S
      with type t = t
       and type step = string
       and type key = string list
       and type contents = string) (store : t Lwt.t) =
  ( module struct
    include Store

    let compare_trees l r = Store.Tree.hash l = Store.Tree.hash r

    let v = Lwt_main.run store

    let find_or_create_tree tree path =
      Store.Tree.find_tree tree path
      |> Lwt.map (Option.value ~default:Store.Tree.empty)

    let distribute key (metadata : Metadata_file.t) =
      let open Let_syntax (Lwt) in
      let insert (root : Store.tree option) =
        let root =
          match root with
          | None -> Store.Tree.empty
          | Some tree -> tree
        and insert_file (tree, count) { File.digest; in_the_cache; _ } =
          let contents = Io.read_file in_the_cache in
          let+ new_tree =
            Store.Tree.add tree [ Digest.to_string digest ] contents
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
        let author = "dune-cache <https://github.com/ocaml/dune>"
        and date = Int64.of_float (Unix.gettimeofday ())
        and message =
          Format.asprintf "Promotion of rule %s" (Digest.to_string key)
        in
        Irmin.Info.v ~author ~date message
      in
      let store =
        Lwt.map
          (Result.map_error ~f:(fun _ -> "FIXME irmin write error"))
          (Store.with_tree ~info v [] insert)
      in
      Lwt_main.run store

    let write_file ~binary path contents =
      Path.mkdir_p @@ Path.parent_exn path;
      Io.write_file ~binary path contents

    let search_missing_file ~of_path ~of_string path dir key =
      if Path.exists path then (
        Log.info
          [ Pp.textf "file already present locally: %s" (Digest.to_string key) ];
        let f d = Some (d, None) in
        of_path path |> Result.map ~f |> Lwt.return
      ) else
        let open Let_syntax (Lwt) in
        Store.find v [ dir; Digest.to_string key ] >|= function
        | None ->
          Log.info
            [ Pp.textf "file not found in the distributed cache: %s"
                (Digest.to_string key)
            ];
          Result.Ok None
        | Some contents ->
          Log.info [ Pp.textf "retrieved file %s" (Digest.to_string key) ];
          let f d = Some (d, Some contents) in
          of_string contents |> Result.map ~f

    let rec prefetch key =
      let open Let_syntax (Lwt_result) in
      let retrieve =
        let path = Cache.Local.path_metadata cache key in
        let* metadata =
          search_missing_file ~of_path:Metadata_file.parse
            ~of_string:Metadata_file.of_string path "meta" key
        in
        match metadata with
        | None -> Result.Ok () |> Lwt.return
        | Some (metadata, contents) ->
          let* () = prefetch_data metadata in
          Option.iter ~f:(write_file ~binary:false path) contents;
          Lwt.return (Result.Ok ())
      in
      Lwt_main.run retrieve

    and prefetch_data (metadata : Metadata_file.t) =
      let open Let_syntax (Lwt_result) in
      let retrieve_file (f : File.t) =
        let path = Cache.Local.path_data cache f.digest in
        let+ contents =
          search_missing_file
            ~of_path:(fun _ -> Result.Ok ())
            ~of_string:(fun _ -> Result.Ok ())
            path "files" f.digest
        in
        match contents with
        | Some ((), Some contents) -> write_file ~binary:true path contents
        | _ -> ()
      in
      let open Let_syntax (Lwt) in
      let+ results = Lwt_list.map_p retrieve_file metadata.files in
      Result.List.all results |> Result.map ~f:ignore
  end : S )

let irmin local =
  let module Store = Irmin_mem.KV (Irmin.Contents.String) in
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
