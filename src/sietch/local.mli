(** Slightly augmented version of dune's Cache.Local. *)

open Stdune

type t

val make : Cache.Local.t -> t

val promote_sync :
     t
  -> (Path.Build.t * Digest.t) list
  -> Cache.Key.t
  -> Cache.metadata
  -> repository:int option
  -> duplication:Cache.Duplication_mode.t option
  -> ( Cache.Local.Metadata_file.t * Cache.promotion list
     , string )
     Async.Deferred.Result.t

val tmp : t -> Path.t

val throttle_fd : t -> (unit -> 'a Async.Deferred.t) -> 'a Async.Deferred.t

val file_path : t -> Cache.Key.t -> Path.t

val metadata_path : t -> Cache.Key.t -> Path.t

val set_build_dir : t -> Path.t -> (t, string) Result.t

val with_repositories : t -> Cache.repository list -> (t, string) Result.t

val teardown : t -> unit
