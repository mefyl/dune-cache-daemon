include Distributed_intf
module Metadata_file = Cache.Local.Metadata_file
module Log = Dune_util.Log

let disabled _ =
  ( module struct
    let distribute _ _ = Async.Deferred.Result.return ()

    let prefetch _ = Async.Deferred.Result.return ()

    let index_add _ _ _ = Async.Deferred.Result.return ()

    let index_prefetch _ _ = Async.Deferred.Result.return ()
  end : S )
