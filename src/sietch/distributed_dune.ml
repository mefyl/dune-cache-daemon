open Stdune
open Utils
module Sexp = Sexplib.Sexp

type t =
  { cache : Local.t
  ; uri : Uri.t
  }

let ( let* ) = Lwt_result.( >>= )

module Config = struct
  type t = { nodes : node list }

  and node =
    { hostname : string
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
          | None -> Result.return (Some h, ranges)
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
      try Result.return @@ Sexp.load_sexps @@ Path.to_string path
      with e -> Result.Error ("sexp parse error: " ^ Printexc.to_string e)
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
        ( "unrecognized configuration file first statement: "
        ^ Sexp.to_string head )
    | [] -> Result.Error "empty configuration file"
end

let make cache uri = { cache; uri }

let debug = Dune_util.Log.info

let call t m ?body path =
  let uri = Uri.with_uri ~path:(Option.some @@ Uri.path t.uri ^ path) t.uri
  and headers =
    Cohttp.Header.of_list [ ("Content-Type", "application/octet-stream") ]
  in
  let* response, body =
    try%lwt
      Cohttp_lwt_unix.Client.call m ~headers ?body uri |> Lwt.map Result.ok
    with Unix.Unix_error (e, f, a) ->
      Lwt_result.fail
        (Printf.sprintf "error during HTTP request %s: %s %s"
           (Unix.error_message e) f a)
  in
  let* body = Cohttp_lwt.Body.to_string body |> Lwt.map Result.ok in
  Lwt_result.return (Cohttp.Response.status response, body)

let expect_status expected m path = function
  | effective when List.exists ~f:(( = ) effective) expected -> Result.Ok ()
  | effective ->
    Result.Error
      (Format.sprintf "unexpected %s on %s %s"
         (Cohttp.Code.string_of_status effective)
         (Cohttp.Code.string_of_method m)
         path)

let put_contents t path contents =
  let body = Lwt_stream.of_list [ contents ] |> Cohttp_lwt.Body.of_stream in
  let* status, _body = call t `PUT ~body path in
  Lwt.return @@ expect_status [ `Created; `OK ] `PUT path status

let get_file t path local_path =
  if Path.exists local_path then
    debug
      [ Pp.textf "metadata file already present locally: %s"
          (Path.to_string local_path)
      ]
    |> Lwt_result.return
  else
    let* status, body = call t `GET path in
    let* () = expect_status [ `OK ] `GET path status |> Lwt.return in
    write_file t.cache local_path true body

let distribute ({ cache; _ } as t) key (metadata : Cache.Local.Metadata_file.t)
    =
  try%lwt
    let insert_file { Cache.File.digest; _ } =
      let path = Local.file_path cache digest in
      let insert input =
        let body =
          let read () =
            let%lwt res = Lwt_io.read ~count:512 input in
            if res <> "" then
              Some res |> Lwt.return
            else
              None |> Lwt.return
          in
          Lwt_stream.from read |> Cohttp_lwt.Body.of_stream
        in
        let path = "blocks/" ^ Digest.to_string digest in
        let* status, _body = call t `PUT ~body path in
        Lwt.return @@ expect_status [ `Created; `No_content ] `PUT path status
      in
      Lwt_io.with_file ~mode:Lwt_io.input (Path.to_string path) insert
    in
    let%lwt results = Lwt.all @@ List.map ~f:insert_file metadata.files in
    let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
    put_contents t
      ("blocks/" ^ Digest.to_string key)
      (Cache.Local.Metadata_file.to_string metadata)
  with e -> failwith ("distribute fatal error: " ^ Printexc.to_string e)

let prefetch ({ cache; _ } as t) key =
  try%lwt
    let local_path = Local.metadata_path cache key in
    if Path.exists local_path then
      debug
        [ Pp.textf "metadata file already present locally: %s"
            (Path.to_string local_path)
        ]
      |> Lwt_result.return
    else
      let path = "blocks/" ^ Digest.to_string key in
      let* status, body = call t `GET path in
      let* () = expect_status [ `OK ] `GET path status |> Lwt.return in
      let* metadata = Cache.Local.Metadata_file.of_string body |> Lwt.return in
      let fetch { Cache.File.digest; _ } =
        get_file t
          ("blocks/" ^ Digest.to_string digest)
          (Local.file_path cache digest)
      in
      let%lwt results = Lwt.all @@ List.map ~f:fetch metadata.files in
      let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
      write_file cache local_path false body
  with e -> failwith ("prefetch fatal error: " ^ Printexc.to_string e)

let index_path name key =
  String.concat ~sep:"/" [ "index"; name; Digest.to_string key ]

let index_add t name key keys =
  try%lwt
    put_contents t (index_path name key)
      (String.concat ~sep:"\n" (List.map ~f:Digest.to_string keys))
  with e -> failwith ("index_add fatal error: " ^ Printexc.to_string e)

let index_prefetch t name key =
  try%lwt
    let path = index_path name key in
    let* status, body = call t `GET path in
    let* () =
      expect_status [ `OK; `Not_found ] `GET path status |> Lwt.return
    in
    if status = `OK then
      let keys =
        String.split ~on:'\n' body
        |> List.map ~f:(fun d -> Digest.from_hex d |> Option.value_exn)
      in
      let%lwt results = Lwt_list.map_s (prefetch t) keys in
      let* (_ : unit list) = Result.List.all results |> Lwt.return in
      Lwt_result.return ()
    else
      Lwt_result.return ()
  with e -> failwith ("index_prefetech fatal error: " ^ Printexc.to_string e)

let make uri local =
  ( module struct
    let v =
      let local = Local.make local in
      make local uri

    let distribute = distribute v

    let prefetch = prefetch v

    let index_add = index_add v

    let index_prefetch = index_prefetch v
  end : Distributed.S )
