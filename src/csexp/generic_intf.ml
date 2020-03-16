open Utils

module type Stream = sig
  type t

  module Monad : Monad
end

module type IStream = sig
  include Stream

  val peek : t -> char option Monad.t

  val next : t -> char option Monad.t
end

module type OStream = sig
  include Stream

  val send : t -> string -> unit Monad.t
end
