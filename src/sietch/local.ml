open Stdune

let ( let* ) = Result.( >>= )

type t =
  { cache : Cache.Local.t
  ; throttle : unit Async.Throttle.t
  ; tmp : Path.t
  }

let make cache =
  { cache
  ; throttle =
      Async.Throttle.create ~continue_on_error:true ~max_concurrent_jobs:256
  ; tmp = Cache.Local.tmp_path cache "dune-cache-daemon"
  }

let tmp { tmp; _ } = tmp

let promote_sync { cache; throttle; _ } files key metadata ~repository
    ~duplication =
  let f () =
    Async.return
    @@ Cache.Local.promote_sync cache files key metadata ~repository
         ~duplication
  in
  Async.Throttle.enqueue throttle f

let throttle_fd { throttle; _ } f = Async.Throttle.enqueue throttle f

let file_path { cache; _ } = Cache.Local.file_path cache

let metadata_path { cache; _ } = Cache.Local.metadata_path cache

let set_build_dir t path =
  let* cache = Cache.Local.set_build_dir t.cache path in
  Result.Ok { t with cache }

let with_repositories t repos =
  let* cache = Cache.Local.with_repositories t.cache repos in
  Result.Ok { t with cache }

let teardown { cache; _ } = Cache.Local.teardown cache
