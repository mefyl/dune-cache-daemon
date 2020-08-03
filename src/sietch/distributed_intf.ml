open Cache

module type S = sig
  val distribute :
    Key.t -> Local.Metadata_file.t -> (unit, string) Async.Deferred.Result.t

  val prefetch : Key.t -> (unit, string) Async.Deferred.Result.t

  val index_add :
    string -> Key.t -> Key.t list -> (unit, string) Async.Deferred.Result.t

  val index_prefetch : string -> Key.t -> (unit, string) Async.Deferred.Result.t
end
