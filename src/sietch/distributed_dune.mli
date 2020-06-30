open Stdune

module Config : sig
  type t = { nodes : node list }

  and node =
    { hostname : string
    ; space : range list
    }

  and range = Digest.t * Digest.t

  val of_file : Path.t -> (t, string) Result.t
end

val make : Uri.t -> Cache.Local.t -> (module Distributed.S)
