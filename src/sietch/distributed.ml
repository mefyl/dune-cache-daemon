open Stdune
open Cache
include Distributed_intf
module Metadata_file = Cache.Local.Metadata_file

let disabled =
  ( module struct
    type t = unit

    let v = ()

    let distribute _ _ = Result.Ok ()

    let prefetch _ = Result.Ok 0
  end : S )

let _irmin (type t) cache
    (module Store : Irmin.S
      with type t = t
       and type step = string
       and type key = string list
       and type contents = string) (store : t Lwt.t) =
  ( module struct
    include Store

    let v = Lwt_main.run store

    let ( let* ) = Lwt.bind

    let ( let+ ) = Lwt.Infix.( >|= )

    let find_or_create_tree tree path =
      Store.Tree.find_tree tree path
      |> Lwt.map (Option.value ~default:Store.Tree.empty)

    let distribute key (metadata : Metadata_file.t) =
      let insert (root : Store.tree option) =
        let root =
          match root with
          | None -> Store.Tree.empty
          | Some tree -> tree
        and insert_file tree { File.digest; in_the_cache; _ } =
          let contents = Io.read_file in_the_cache in
          Store.Tree.add tree [ Digest.to_string digest ] contents
        in
        let* tree_files =
          let* tree_files = find_or_create_tree root [ "files" ] in
          Lwt_list.fold_left_s insert_file tree_files metadata.files
        in
        let* tree_metadata =
          let* tree_metadata = find_or_create_tree root [ "meta" ] in
          Store.Tree.add tree_metadata [ Digest.to_string key ]
            (Metadata_file.to_string metadata)
        in
        let* root = Store.Tree.add_tree root [ "files" ] tree_files in
        let* root = Store.Tree.add_tree root [ "meta" ] tree_metadata in
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

    let prefetch_data (metadata : Metadata_file.t) =
      let retrieve_file (f : File.t) =
        let path = Cache.Local.path_metadata cache f.digest in
        let+ contents = Store.get v [ "files"; Digest.to_string f.digest ] in
        Io.write_file ~binary:true path contents
      in
      Lwt_list.iter_p retrieve_file metadata.files

    let prefetch key =
      let open Lwt.Infix in
      let retrieve =
        Store.find v [ "meta"; Digest.to_string key ] >>= function
        | None -> Lwt.return (Result.Ok 0)
        | Some contents ->
          let path = Cache.Local.path_metadata cache key in
          if not (Path.exists path) then
            match Metadata_file.of_string contents with
            | Result.Ok metadata ->
              let* () = prefetch_data metadata in
              Io.write_file ~binary:false path contents;
              Lwt.return (Result.Ok 1)
            | Result.Error e -> Lwt.return (Result.Error e)
          else
            Lwt.return (Result.Ok 0)
      in
      Lwt_main.run retrieve
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
