open Stdune

type t = { nodes : node list }

and node =
  { hostname : Uri.t
  ; space : range list
  }

and range = Digest.t * Digest.t

val of_file : Path.t -> (t, string) Result.t
