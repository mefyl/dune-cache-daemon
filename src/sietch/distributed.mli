include module type of Distributed_intf

val disabled : (module S)

val irmin : Cache.Local.t -> (module S)

val irmin_git : Cache.Local.t -> Stdune.Path.t -> (module S)
