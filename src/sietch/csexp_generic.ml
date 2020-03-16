open Stdune
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

module Generic
    (IMonad : Monad)
    (IStream : IStream with module Monad = IMonad)
    (OMonad : Monad)
    (OStream : OStream with module Monad = OMonad) =
struct
  type t = Sexp.t

  let peek s =
    let open Let_syntax (IMonad) in
    IStream.peek s >|= function
    | Some v -> Result.Ok v
    | None -> Error (`Parse_error "unexpected end of file")

  let read_string s l =
    let open Let_syntax (IMonad) in
    let res = Bytes.make l ' ' in
    let rec read v =
      if v = l then
        Result.Ok () |> IMonad.return
      else
        IStream.next s >>= function
        | Some c ->
          BytesLabels.set res v c;
          read (v + 1)
        | None ->
          Result.Error (`Parse_error "unterminated string") |> IMonad.return
    in
    let+ read = read 0 in
    Result.map ~f:(fun () -> Bytes.to_string res) read

  module IMonad_stack = struct
    let ( let* ) v f =
      IMonad.bind v (function
        | Result.Ok v -> f v
        | Result.Error _ as e -> IMonad.return e)

    let ( let+ ) v f =
      IMonad.bind v (function
        | Result.Ok v -> f v |> Result.ok |> IMonad.return
        | Result.Error _ as e -> IMonad.return e)
  end

  let parse stream =
    let open Let_syntax (IMonad) in
    let rec read_size acc =
      IStream.next stream >>= function
      | Some c ->
        if c = ':' then
          Result.Ok acc |> IMonad.return
        else
          let idx = int_of_char c - int_of_char '0' in
          if idx < 0 || idx > 9 then
            Result.Error
              (`Parse_error
                (Printf.sprintf "invalid character in size: %s"
                   (Char.escaped c)))
            |> IMonad.return
          else
            read_size ((10 * acc) + idx)
      | None ->
        Result.Error (`Parse_error "unexpected EOF in size") |> IMonad.return
    in
    let rec parse () =
      IStream.peek stream >>= function
      | Some '(' ->
        let* _ = IStream.next stream in
        let open IMonad_stack in
        let+ l = parse_list () in
        Sexp.List l
      | Some _ ->
        let open IMonad_stack in
        let* size = read_size 0 in
        let+ string = read_string stream size in
        Sexp.Atom string
      | None ->
        Result.Error (`Parse_error "expected s-expression") |> IMonad.return
    and parse_list () =
      IStream.peek stream >>= function
      | Some ')' ->
        let* _ = IStream.next stream in
        [] |> Result.ok |> IMonad.return
      | Some ':' -> Result.Error (`Parse_error "missing size") |> IMonad.return
      | Some _ ->
        let open IMonad_stack in
        let* head = parse () in
        let+ tail = parse_list () in
        head :: tail
      | None ->
        Result.Error (`Parse_error "expected s-expression content")
        |> IMonad.return
    in
    parse ()

  let serialize stream sexp =
    let rec loop =
      let open Let_syntax (OMonad) in
      function
      | Sexp.Atom str ->
        let* () = OStream.send stream (string_of_int (String.length str)) in
        let* () = OStream.send stream ":" in
        OStream.send stream str
      | Sexp.List e ->
        let* () = OStream.send stream "(" in
        let* () =
          let f acc s =
            let* () = acc in
            loop s
          in
          List.fold_left ~init:(OMonad.return ()) ~f e
        in
        OStream.send stream ")"
    in
    loop sexp

  (* let parse_string string =
   *   let open Result.O in
   *   let stream = Stream.of_string string in
   *   let* result = parse stream in
   *   match Stream.peek stream with
   *   | Some _ -> Error "not whole string consumed"
   *   | None -> Ok result *)
end

module Lwt_istream = struct
  type error = [ `Read_error of string ]

  let open_error = function
    | Result.Ok v -> Result.Ok v
    | Result.Error (`Read_error e) -> Result.Error (`Read_error e)

  type t =
    { channel : Lwt_io.input Lwt_io.channel
    ; mutable peek : char option
    }

  let make channel = { channel; peek = None }

  module Monad = struct
    include Lwt_result

    type nonrec 'a t = ('a, error) t
  end

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

module Lwt_ostream = struct
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

  let send stream str =
    try%lwt Lwt_io.write stream str |> Lwt.map Result.ok
    with Unix.Unix_error (e, f, _) ->
      Lwt_result.fail
        (`Write_error (Printf.sprintf "%s: %s" (Unix.error_message e) f))
end

module Lwt_stacked =
  Generic (Lwt_istream.Monad) (Lwt_istream) (Lwt_ostream.Monad) (Lwt_ostream)

module Parser_lwt = struct
  let parse stream =
    let open Let_syntax (Lwt) in
    let+ res = Lwt_stacked.parse stream in
    match res with
    | Result.Ok (Result.Ok v) -> Result.Ok v
    | Result.Ok (Result.Error csexp_error) -> Result.Error csexp_error
    | Result.Error _ as e -> Lwt_istream.open_error e

  let serialize stream s =
    let open Let_syntax (Lwt) in
    let+ res = Lwt_stacked.serialize stream s in
    match res with
    | Result.Ok v -> Result.Ok v
    | Result.Error _ as e -> Lwt_ostream.open_error e
end
