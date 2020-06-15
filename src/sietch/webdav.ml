open Stdune
open Utils

type t =
  { cache : Local.t
  ; uri : Uri.t
  }

let make cache uri = { cache; uri }

let ( let* ) = Lwt_result.( >>= )

let debug = Dune_util.Log.info

(* let check_status m uri response =
 *   match Cohttp.Response.status response with
 *   | `OK
 *   | `No_content ->
 *     Lwt_result.return ()
 *   | _ ->
 *     let status = Cohttp.Response.status response |> Cohttp.Code.string_of_status
 *     and m = Cohttp.Code.string_of_method m
 *     and uri = Uri.to_string uri in
 *     Lwt_result.fail (Format.sprintf "unexpected %s on %s %s" status m uri) *)

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
  Lwt.return @@ expect_status [ `Created; `No_content ] `PUT path status

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
        let path = "files/v3/" ^ Digest.to_string digest in
        let* status, _body = call t `PUT ~body path in
        Lwt.return @@ expect_status [ `Created; `No_content ] `PUT path status
      in
      Lwt_io.with_file ~mode:Lwt_io.input (Path.to_string path) insert
    in
    let%lwt results = Lwt.all @@ List.map ~f:insert_file metadata.files in
    let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
    put_contents t
      ("meta/v3/" ^ Digest.to_string key)
      (Cache.Local.Metadata_file.to_string metadata)
  with _ -> failwith "distribute fatal error"

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
      let path = "meta/v3/" ^ Digest.to_string key in
      let* status, body = call t `GET path in
      let* () = expect_status [ `OK ] `GET path status |> Lwt.return in
      let* metadata = Cache.Local.Metadata_file.of_string body |> Lwt.return in
      let fetch { Cache.File.digest; _ } =
        get_file t
          ("files/v3/" ^ Digest.to_string digest)
          (Local.file_path cache digest)
      in
      let%lwt results = Lwt.all @@ List.map ~f:fetch metadata.files in
      let* (_ : unit list) = results |> Result.List.all |> Lwt.return in
      write_file cache local_path false body
  with _ -> failwith "prefetch fatal error"

let index_path name key =
  String.concat ~sep:"/" [ "index"; name; Digest.to_string key ]

let index_add t name key keys =
  try%lwt
    put_contents t (index_path name key)
      (String.concat ~sep:"\n" (List.map ~f:Digest.to_string keys))
  with _ -> failwith "index_add fatal error"

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
  with _ -> failwith "index_prefetch fatal error"

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
