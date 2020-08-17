module Csexp_external = Csexp
open Stdune

module Csexp = struct
  include Csexp_external.Make (Sexp)
end

let ( >>= ) = Async.Deferred.Result.( >>= )

let ( >>| ) = Async.Deferred.Result.( >>| )

let ( let* ) = ( >>= )

let ( let+ ) = ( >>| )

let int_of_string ?where s =
  match Int.of_string s with
  | Some s -> Ok s
  | None ->
    Result.Error
      (Printf.sprintf "invalid integer%s: %s"
         ( match where with
         | Some l -> " in " ^ l
         | None -> "" )
         s)

let retry ?message ?(count = 100) f =
  let rec loop = function
    | x when x >= count ->
      Result.Error
        (Failure
           ( Printf.sprintf "too many retries (%i)" x
           ^
           match message with
           | None -> ""
           | Some msg -> ": " ^ msg ))
    | x -> (
      match f () with
      | Some v -> Result.Ok v
      | None ->
        Thread.delay 0.1;
        loop (x + 1) )
  in
  loop 0

let read_char input =
  let f () = Async.Reader.read_char input in
  let open Async in
  Async.try_with ~extract_exn:true f >>= function
  | Result.Ok (`Ok c) -> Deferred.Result.return @@ Some c
  | Result.Ok `Eof -> Deferred.Result.return None
  | Result.Error End_of_file -> Deferred.Result.return None
  | Result.Error e -> Deferred.Result.fail (`Read_error (Printexc.to_string e))

let read_char_or_fail input =
  let open Async.Deferred.Result in
  read_char input >>= function
  | Some c -> Async.Deferred.Result.return c
  | None -> Async.Deferred.Result.fail (`Parse_error "premature end of input")

let rec read_token i lexer c stack =
  let open Csexp.Parser.Lexer in
  match feed lexer c with
  | Lparen ->
    Async.Deferred.Result.return @@ Csexp.Parser.Stack.open_paren stack
  | Rparen ->
    Async.Deferred.Result.return @@ Csexp.Parser.Stack.close_paren stack
  | Atom count -> (
    let str = String.make count '\x00' in
    let f () =
      let bytes = Bytes.unsafe_of_string str in
      Async.Reader.really_read i bytes
    in
    let open Async in
    Async.try_with ~extract_exn:true f >>= function
    | Result.Ok `Ok ->
      Async.Deferred.Result.return (Csexp.Parser.Stack.add_atom str stack)
    | Result.Ok (`Eof _) ->
      Async.Deferred.Result.fail (`Parse_error "premature end of input")
    | Result.Error e ->
      Async.Deferred.Result.fail (`Read_error (Printexc.to_string e)) )
  | Await ->
    let* c = read_char_or_fail i in
    read_token i lexer c stack

let parse_sexp ?c input =
  let open Async.Deferred.Result in
  let lexer = Csexp.Parser.Lexer.create () in
  let* c =
    match c with
    | Some c -> Async.Deferred.Result.return c
    | None -> read_char_or_fail input
  in
  let rec loop c stack =
    read_token input lexer c stack >>= function
    | Sexp (s, Empty) -> Async.Deferred.Result.return s
    | stack ->
      let* c = read_char_or_fail input in
      loop c stack
  in
  loop c Csexp.Parser.Stack.Empty

let mkdir p =
  let f () = Async.Unix.mkdir ~perm:0o700 p in
  let ( >>| ) = Async.Deferred.( >>| ) in
  Async.try_with ~extract_exn:true f >>| function
  | Result.Ok () -> ()
  | Result.Error (Unix.Unix_error (Unix.EEXIST, _, _)) -> ()
  | Result.Error e -> raise e

(** Write file in an atomic manner. *)
let write_file local path executable contents =
  let f () =
    let dir = path |> Path.parent_exn |> Path.to_string
    and path_tmp =
      path |> Path.basename |> Path.relative (Local.tmp local) |> Path.to_string
    and path = path |> Path.to_string
    and perm =
      if executable then
        0o500
      else
        0o400
    in
    let ( let* ) = Async.Deferred.( >>= )
    and ( and* ) = Async.Deferred.both in
    let* () =
      let write () =
        let* output = Async.Writer.open_file ~perm path_tmp in
        let () = Async.Writer.write output contents in
        Async.Writer.close output
      in
      Local.throttle_fd local write
    and* () = mkdir dir in
    Async.Unix.rename ~src:path_tmp ~dst:path
  in
  let ( >>= ) = Async.Deferred.( >>= ) in
  Async.try_with ~extract_exn:true f >>= function
  | Result.Ok () -> Async.Deferred.Result.return ()
  | Result.Error (Unix.Unix_error (Unix.EACCES, _, _)) ->
    (* If the file exists with no write permissions, it is being pulled as part
       of another hinting. *)
    Async.Deferred.Result.return ()
  | Result.Error (Unix.Unix_error (e, f, a)) ->
    Async.Deferred.Result.fail
      (Printf.sprintf "%s: %s %s" (Unix.error_message e) f a)
  | Result.Error e -> raise e
