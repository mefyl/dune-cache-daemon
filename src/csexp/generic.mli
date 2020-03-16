open Utils

include module type of Generic_intf

module Make
    (IMonad : Monad)
    (IStream : IStream with module Monad = IMonad)
    (OMonad : Monad)
    (OStream : OStream with module Monad = OMonad) : sig
  val parse :
    IStream.t -> (Stdune.Sexp.t, [> `Parse_error of string ]) result IMonad.t

  val serialize : OStream.t -> Stdune.Sexp.t -> unit OMonad.t
end
