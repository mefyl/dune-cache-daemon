include module type of Distributed_intf

val disabled : Cache.Local.t -> (module S)

val irmin : Cache.Local.t -> (module S)

val irmin_git : Stdune.Path.t -> Cache.Local.t -> (module S)
