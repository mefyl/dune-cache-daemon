open Stdune
open Utils

module IMonad = struct
  include Lwt_result

  type error = [ `Read_error of string ]

  type nonrec 'a t = ('a, error) t
end

module IStream = struct
  module Monad = IMonad

  type error =
    [ `Parse_error of string
    | `Read_error of string
    ]

  let open_error = function
    | Result.Ok v -> Result.Ok v
    | Result.Error (`Read_error e) -> Result.Error (`Read_error e)

  type t =
    { channel : Lwt_io.input Lwt_io.channel
    ; mutable peek : char option
    }

  let make channel = { channel; peek = None }

  let to_channel { channel; _ } = channel

  let lwt_read stream =
    try%lwt
      let open Let_syntax (Lwt) in
      let+ c = Lwt_io.read_char stream.channel in
      Result.Ok (Some c)
    with
    | End_of_file -> Lwt_result.return None
    | Unix.Unix_error (e, f, _) ->
      Error (`Read_error (Printf.sprintf "%s: %s" (Unix.error_message e) f))
      |> Lwt.return

  let peek stream =
    match stream.peek with
    | None ->
      let ( let+ ) = Lwt_result.Infix.( >|= ) in
      let+ c = lwt_read stream in
      stream.peek <- c;
      c
    | c -> Lwt_result.return c

  let next stream =
    match stream.peek with
    | None -> lwt_read stream
    | c ->
      stream.peek <- None;
      Lwt_result.return c
end

module OStream = struct
  type error = [ `Write_error of string ]

  let open_error = function
    | Result.Ok v -> Result.Ok v
    | Result.Error (`Write_error e) -> Result.Error (`Write_error e)

  module Monad = struct
    include Lwt_result

    type nonrec 'a t = ('a, error) t
  end

  type t = Lwt_io.output Lwt_io.channel

  let make channel = channel

  let to_channel channel = channel

  let send stream str =
    try%lwt Lwt_io.write stream str |> Lwt.map Result.ok
    with Unix.Unix_error (e, f, _) ->
      Lwt_result.fail
        (`Write_error (Printf.sprintf "%s: %s" (Unix.error_message e) f))
end

module Lwt_stacked =
  Generic.Make (IStream.Monad) (IStream) (OStream.Monad) (OStream)

let parse stream =
  let open Let_syntax (Lwt) in
  let+ res = Lwt_stacked.parse stream in
  match res with
  | Result.Ok (Result.Ok v) -> Result.Ok v
  | Result.Ok (Result.Error csexp_error) -> Result.Error csexp_error
  | Result.Error _ as e -> IStream.open_error e

let serialize stream s =
  let open Let_syntax (Lwt) in
  let+ res = Lwt_stacked.serialize stream s in
  match res with
  | Result.Ok v -> Result.Ok v
  | Result.Error _ as e -> OStream.open_error e
