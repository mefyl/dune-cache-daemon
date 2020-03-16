open Stdune

module IStream : sig
  type t

  type error =
    [ `Parse_error of string
    | `Read_error of string
    ]

  val make : Lwt_io.input Lwt_io.channel -> t

  val next : t -> (char option, [> error ]) Lwt_result.t

  val peek : t -> (char option, [> error ]) Lwt_result.t

  val to_channel : t -> Lwt_io.input Lwt_io.channel
end

module OStream : sig
  type t

  type error = [ `Write_error of string ]

  val make : Lwt_io.output Lwt_io.channel -> t

  val to_channel : t -> Lwt_io.output Lwt_io.channel
end

val parse : IStream.t -> (Sexp.t, [> IStream.error ]) Lwt_result.t

val serialize : OStream.t -> Sexp.t -> (unit, [> OStream.error ]) Lwt_result.t
