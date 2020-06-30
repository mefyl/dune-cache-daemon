open Stdune

module Config : sig
  type t = { nodes : node list }

  and node =
    { hostname : Uri.t
    ; space : range list
    }

  and range = Digest.t * Digest.t

  val of_file : Path.t -> (t, string) Result.t
end

val make : Config.t -> Cache.Local.t -> (module Distributed.S)
