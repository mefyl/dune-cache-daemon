open Stdune

type t =
  { cache : Cache.Local.t
  ; fd_pool : unit Lwt_pool.t
  ; tmp : Path.t
  }

let make cache =
  { cache
  ; fd_pool = Lwt_pool.create 128 (fun () -> Lwt.return ())
  ; tmp = Cache.Local.tmp_path cache "dune-cache-daemon"
  }

let tmp { tmp; _ } = tmp

let throttle_fd { fd_pool; _ } f = Lwt_pool.use fd_pool f

let file_path { cache; _ } = Cache.Local.file_path cache

let metadata_path { cache; _ } = Cache.Local.metadata_path cache
