open Stdune
module Sexp = Sexplib.Sexp

type t = { nodes : node list }

and node =
  { hostname : Uri.t
  ; space : range list
  }

and range = Digest.t * Digest.t

let parse_address a pad =
  match String.drop_prefix ~prefix:"0x" a with
  | None -> Result.Error ("address must start with \"0x\": " ^ a)
  | Some a -> Result.return (a ^ String.make 28 pad)

let node_of_sexp = function
  | Sexp.List (Sexp.Atom "node" :: contents) -> (
    let ( let* ) v f = Result.bind v ~f in
    let f (hostname, ranges) = function
      | Sexp.List [ Sexp.Atom "hostname"; Sexp.Atom h ] -> (
        match hostname with
        | None -> Result.return (Some (Uri.of_string h), ranges)
        | Some _ -> Result.Error ("duplicate hostname statement: " ^ h) )
      | Sexp.List [ Sexp.Atom "range"; Sexp.Atom start; Sexp.Atom end_ ] -> (
        let* start = parse_address start '0' in
        let* end_ = parse_address end_ 'f' in
        match (Digest.from_hex start, Digest.from_hex end_) with
        | Some start, Some end_ ->
          Result.return (hostname, (start, end_) :: ranges)
        | None, _ -> Result.Error ("invalid address: " ^ start)
        | _, None -> Result.Error ("invalid address: " ^ end_) )
      | stmt -> Result.Error ("invalid node statement: " ^ Sexp.to_string stmt)
    in
    let* hostname, ranges =
      Result.List.fold_left ~f ~init:(None, []) contents
    in
    match (hostname, ranges) with
    | Some hostname, space -> Result.return { hostname; space }
    | None, _ -> Result.Error "missing node hostname" )
  | stmt -> Result.Error ("invalid config statement: " ^ Sexp.to_string stmt)

let of_file path =
  let ( let* ) v f = Result.bind v ~f in
  let* sexp =
    try Result.return @@ Sexp.load_sexps @@ Path.to_string path with
    | Sys_error e ->
      Result.Error (Printf.sprintf "unable to load configuration: %s" e)
    | e -> Result.Error ("sexp parse error: " ^ Printexc.to_string e)
  in
  match sexp with
  | Sexp.List
      [ Sexp.Atom "lang"
      ; Sexp.Atom "dune-distributed-cache"
      ; Sexp.Atom version
      ]
    :: contents ->
    let* () =
      match String.split ~on:'.' version with
      | [ major; minor ] -> (
        match (Int.of_string major, Int.of_string minor) with
        | Some 1, Some 0 -> Result.return ()
        | Some _, Some _ -> Result.Error ("unsupported version: " ^ version)
        | _ -> Result.Error ("invalid version: " ^ version) )
      | _ -> Result.Error ("invalid version: " ^ version)
    in
    let* nodes = Result.List.map ~f:node_of_sexp contents in
    Result.return { nodes }
  | head :: _ ->
    Result.Error
      ("unrecognized configuration file first statement: " ^ Sexp.to_string head)
  | [] -> Result.Error "empty configuration file"
