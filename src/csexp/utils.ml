module type Monad = sig
  type 'a t

  val return : 'a -> 'a t

  val bind : 'a t -> ('a -> 'b t) -> 'b t

  val map : ('a -> 'b) -> 'a t -> 'b t

  module Infix : sig
    val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t

    val ( >|= ) : 'a t -> ('a -> 'b) -> 'b t
  end
end

module Let_syntax (O : Monad) = struct
  include O.Infix

  let ( let* ) = ( >>= )

  let ( let+ ) = ( >|= )
end
