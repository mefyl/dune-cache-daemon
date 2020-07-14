include Distributed_intf
module Metadata_file = Cache.Local.Metadata_file
module Log = Dune_util.Log

let disabled _ =
  ( module struct
    let distribute _ _ = Lwt_result.return ()

    let prefetch _ = Lwt_result.return ()

    let index_add _ _ _ = Lwt_result.return ()

    let index_prefetch _ _ = Lwt_result.return ()
  end : S )
