module Csexp_external = Csexp
open Stdune

module Csexp = struct
  include Csexp_external.Make (Sexp)
end

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

module LwtO = struct
  include Lwt.Infix

  let ( let* ) = ( >>= )

  let ( let+ ) = ( >|= )
end

module LwtrO = struct
  include Lwt_result.Infix

  let ( let* ) = ( >>= )

  let ( let+ ) = ( >|= )
end

let read_char input =
  try%lwt
    let open LwtrO in
    let* c =
      Lwt_io.read_char input
      |> Lwt.map (Base.Fn.compose Result.return Option.some)
    in
    Lwt_result.return c
  with
  | End_of_file -> Lwt_result.return None
  | e -> Lwt_result.fail (`Read_error (Printexc.to_string e))

let read_char_or_fail input =
  let open LwtrO in
  read_char input >>= function
  | Some c -> Lwt_result.return c
  | None -> Lwt_result.fail (`Parse_error "premature end of input")

let rec read_token input lexer c stack =
  let open LwtrO in
  let open Csexp.Parser.Lexer in
  match feed lexer c with
  | Lparen -> Lwt_result.return @@ Csexp.Parser.Stack.open_paren stack
  | Rparen -> Lwt_result.return @@ Csexp.Parser.Stack.close_paren stack
  | Atom count -> (
    try%lwt
      let* atom = Lwt_io.read ~count input |> Lwt.map Result.return in
      Lwt_result.return @@ Csexp.Parser.Stack.add_atom atom stack
    with e -> Lwt_result.fail (`Read_error (Printexc.to_string e)) )
  | Await -> (
    read_char input >>= function
    | Some c -> read_token input lexer c stack
    | None -> Lwt_result.fail (`Parse_error "premature end of input") )

let parse_sexp ?c input =
  let open LwtrO in
  let lexer = Csexp.Parser.Lexer.create () in
  let* c =
    match c with
    | Some c -> Lwt_result.return c
    | None -> read_char_or_fail input
  in
  let rec loop c stack =
    read_token input lexer c stack >>= function
    | Sexp (s, Empty) -> Lwt_result.return s
    | stack ->
      let* c = read_char_or_fail input in
      loop c stack
  in
  loop c Csexp.Parser.Stack.Empty

(** A barrier that let thread throughs after it has been opened for some
    duration. Enables to postpone low priority operations until main operations
    have been silent for some time. *)
module Barrier = struct
  open LwtO

  type t =
    { mutable open_ : bool
    ; mutable time : float
    ; mutable promise : unit Lwt.t
    ; mutable resolve : unit Lwt.u
    ; threshold : float
    }

  let make ?(threshold = 0.1) () =
    let promise, resolve = Lwt.wait () in
    { open_ = true; time = Unix.gettimeofday (); promise; resolve; threshold }

  let open_ = function
    | { open_ = true; _ } -> ()
    | { resolve; _ } as b ->
      let p, r = Lwt.wait () in
      b.open_ <- true;
      b.time <- Unix.gettimeofday ();
      b.promise <- p;
      b.resolve <- r;
      Lwt.wakeup resolve ()

  let close b = b.open_ <- false

  let rec wait = function
    | { open_ = true; time; _ } as b ->
      let now = Unix.gettimeofday () in
      let elapsed = now -. time in
      if elapsed > b.threshold then
        Lwt.return ()
      else
        let%lwt () = Lwt_unix.sleep (b.threshold -. elapsed) in
        wait b
    | { promise; _ } as b ->
      let* () = promise in
      wait b

  let use b f =
    close b;
    match%lwt f () with
    | e ->
      open_ b;
      Lwt.return e
    | exception e ->
      open_ b;
      raise e
end

let mkdir p =
  try%lwt Lwt_unix.mkdir p 0o700
  with Unix.Unix_error (Unix.EEXIST, _, _) -> Lwt.return ()

(** Write file in an atomic manner. *)
let write_file local path executable contents =
  try%lwt
    Lwt.map Result.ok
    @@
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
    let%lwt () =
      let write () =
        let%lwt output = Lwt_io.open_file ~perm ~mode:Lwt_io.output path_tmp in
        let%lwt () = Lwt_io.write output contents in
        Lwt_io.close output
      in
      Local.throttle_fd local write
    and () = mkdir dir in
    Lwt_unix.rename path_tmp path
  with
  | Unix.Unix_error (Unix.EACCES, _, _) ->
    (* If the file exists with no write permissions, it is being pulled as part
       of another hinting. *)
    Lwt_result.return ()
  | Unix.Unix_error (e, f, a) ->
    Lwt_result.fail (Printf.sprintf "%s: %s %s" (Unix.error_message e) f a)
