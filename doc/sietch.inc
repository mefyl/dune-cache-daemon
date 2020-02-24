
(rule
 (with-stdout-to sietch-start.1
  (run %{bin:sietch} start --help=groff)))

(install
 (section man)
 (files sietch-start.1))

(rule
 (with-stdout-to sietch-stop.1
  (run %{bin:sietch} stop --help=groff)))

(install
 (section man)
 (files sietch-stop.1))

