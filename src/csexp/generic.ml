open Stdune
open Utils
include Generic_intf

module Make
    (IMonad : Monad)
    (IStream : IStream with module Monad = IMonad)
    (OMonad : Monad)
    (OStream : OStream with module Monad = OMonad) =
struct
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
end
