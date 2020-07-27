include Lwt.Infix

let ( let* ) = ( >>= )

let string_of_sockaddr = function
  | Unix.ADDR_UNIX p -> p
  | Unix.ADDR_INET (addr, port) ->
    Format.sprintf "%s:%i" (Unix.string_of_inet_addr addr) port
