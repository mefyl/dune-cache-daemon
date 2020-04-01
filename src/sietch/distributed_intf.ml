open Cache

module type S = sig
  type t

  val v : t

  val distribute : Key.t -> Local.Metadata_file.t -> (unit, string) Lwt_result.t

  val prefetch : Key.t -> (unit, string) Lwt_result.t

  val index_add : string -> Key.t -> Key.t list -> (unit, string) Lwt_result.t

  val index_prefetch : string -> Key.t -> (unit, string) Lwt_result.t
end
