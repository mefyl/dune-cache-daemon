open Stdune

type t = { nodes : node list }

and node =
  { hostname : Uri.t
  ; space : range list
  }

and range = Digest.t * Digest.t

val of_file : Path.t -> (t, string) Result.t

val range_total : range

val range_includes : range -> Digest.t -> bool

val ranges_include : range list -> Digest.t -> bool
