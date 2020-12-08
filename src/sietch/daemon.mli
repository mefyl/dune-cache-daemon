open Stdune
module Utils = Utils

type t

exception Error of string

type config = { exit_no_client : bool }

type error =
  [ `Local_cache_error of string
  | `Fatal of string
  ]

val make :
     ?root:Path.t
  -> ?distribution:(Local.t -> (module Distributed.S))
  -> config:config
  -> unit
  -> (t, error) Async.Deferred.Result.t

val default_endpoint_file : unit -> Path.t

val check_endpoint_file :
     ?close:bool
  -> Path.t
  -> ((string * int * Unix.file_descr) option, exn) Result.t

val run :
     ?endpoint_f:(string -> unit)
  -> ?endpoint:Unix.sockaddr
  -> ?trim_period:int
  -> ?trim_size:int64
  -> t
  -> (unit, error) Async.Deferred.Result.t

val endpoint : t -> string option

val daemon :
     root:Path.t
  -> ?distribution:(Local.t -> (module Distributed.S))
  -> config:config
  -> (string -> unit)
  -> unit
