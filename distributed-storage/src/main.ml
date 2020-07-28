open Stdune

let path =
  let parse s = Result.return @@ Path.of_filename_relative_to_initial_cwd s
  and print fmt p = Format.pp_print_string fmt (Path.to_string p) in
  Cmdliner.Arg.conv ~docv:"PATH" (parse, print)

let inet_addr =
  let docv = "HOST"
  and parse str =
    match Unix.inet_addr_of_string str with
    | addr -> Result.Ok addr
    | exception Failure _ -> Result.Error (`Msg ("invalid address: " ^ str))
  and print fmt addr =
    Caml.Format.pp_print_string fmt (Unix.string_of_inet_addr addr)
  in
  Cmdliner.Arg.conv ~docv (parse, print)

let config =
  let doc = "configuration file path" in
  Cmdliner.Arg.(
    value
    & opt (some path) None
    & info ~docv:"PATH" ~doc
        ~env:(env_var "DUNE_CACHE_CONFIG" ~doc)
        [ "config" ])

let port =
  Cmdliner.Arg.(
    value & opt int 80
    & info ~docv:"PORT" ~doc:"port to listen on" [ "p"; "port" ])

and host =
  let doc = "Host to listen on."
  and docv = "HOST" in
  Cmdliner.Arg.(
    value
    & opt inet_addr (Unix.inet_addr_of_string "0.0.0.0")
    & info [ "h"; "host" ] ~docv ~doc)

let root =
  Cmdliner.Arg.(
    required
    & opt (some path) None
    & info ~docv:"ROOT" ~doc:"root directory to server" [ "r"; "root" ])

let trim_period =
  let doc = "time to wait between two trims" in
  Cmdliner.Arg.(
    value & opt int 600
    & info ~docv:"PERIOD" ~doc
        ~env:(env_var "DUNE_CACHE_TRIM_PERIOD" ~doc)
        [ "trim-period" ])

let trim_size =
  let doc = "time to wait between two trims" in
  Cmdliner.Arg.(
    value & opt int64 10_000_000_000L
    & info ~docv:"BYTES" ~doc
        ~env:(env_var "DUNE_CACHE_TRIM_SIZE" ~doc)
        [ "trim-size" ])

let () =
  let () =
    Logs.set_reporter (Logs_fmt.reporter ());
    Logs.set_level (Some Logs.Debug)
  in
  let open Cmdliner in
  let doc = Term.info "dune-cache-distributed-storage" in
  Term.exit
  @@ Term.(
       eval
         ( const Server.run $ config $ host $ port $ root $ trim_period
           $ trim_size
         , doc ))
