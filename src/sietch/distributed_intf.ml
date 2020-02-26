open Stdune
open Cache

module type S = sig
  type t

  val v : t

  val distribute : Key.t -> Local.Metadata_file.t -> (unit, string) Result.t

  val prefetch : Key.t -> (unit, string) Result.t
end
