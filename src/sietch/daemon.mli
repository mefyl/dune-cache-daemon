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
  -> ?distribution:(Cache.Local.t -> (module Distributed.S))
  -> config:config
  -> unit
  -> (t, error) Lwt_result.t

val default_port_file : unit -> Path.t

val check_port_file :
     ?close:bool
  -> Path.t
  -> ((string * int * Unix.file_descr) option, exn) Result.t

val run :
     ?port_f:(string -> unit)
  -> ?port:int
  -> ?trim_period:int
  -> ?trim_size:int
  -> t
  -> unit Lwt.t

val endpoint : t -> string option

val daemon :
     root:Path.t
  -> ?distribution:(Cache.Local.t -> (module Distributed.S))
  -> config:config
  -> (string -> unit)
  -> unit
