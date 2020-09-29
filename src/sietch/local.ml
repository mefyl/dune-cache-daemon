open Stdune

type t =
  { cache : Cache.Local.t
  ; throttle : unit Async.Throttle.t
  ; tmp : Path.t
  }

let make cache =
  { cache
  ; throttle =
      Async.Throttle.create ~continue_on_error:true ~max_concurrent_jobs:128
  ; tmp = Cache.Local.tmp_path cache "dune-cache-daemon"
  }

let tmp { tmp; _ } = tmp

let throttle_fd { throttle; _ } f = Async.Throttle.enqueue throttle f

let file_path { cache; _ } = Cache.Local.file_path cache

let metadata_path { cache; _ } = Cache.Local.metadata_path cache
