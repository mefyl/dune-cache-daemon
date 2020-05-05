
(rule
 (with-stdout-to dune-cache-daemon-start.1
  (run %{bin:dune-cache-daemon} start --help=groff)))

(install
 (section man)
 (files dune-cache-daemon-start.1))

(rule
 (with-stdout-to dune-cache-daemon-stop.1
  (run %{bin:dune-cache-daemon} stop --help=groff)))

(install
 (section man)
 (files dune-cache-daemon-stop.1))

