#!/bin/bash

set -e -o pipefail

SIETCH=$1

CMDS=$("$SIETCH" --help=plain | \
           sed -n '/COMMANDS/,/OPTIONS/p' | sed -En 's/^       ([a-z-]+) ?.*/\1/p')

for cmd in $CMDS; do
    cat <<EOF

(rule
 (with-stdout-to sietch-$cmd.1
  (run %{bin:sietch} $cmd --help=groff)))

(install
 (section man)
 (files sietch-$cmd.1))
EOF
done

echo
