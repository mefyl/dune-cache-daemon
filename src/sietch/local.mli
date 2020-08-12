(** Slightly augmented version of dune's Cache.Local. *)

open Stdune

type t

val make : Cache.Local.t -> t

val tmp : t -> Path.t

val throttle_fd : t -> (unit -> unit Async.Deferred.t) -> unit Async.Deferred.t

val file_path : t -> Cache.Key.t -> Path.t

val metadata_path : t -> Cache.Key.t -> Path.t
