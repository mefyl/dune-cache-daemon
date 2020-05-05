#!/bin/bash

set -e -o pipefail

DUNE_CACHE_DAEMON=$1

CMDS=$("$DUNE_CACHE_DAEMON" --help=plain | \
           sed -n '/COMMANDS/,/OPTIONS/p' | sed -En 's/^       ([a-z-]+) ?.*/\1/p')

for cmd in $CMDS; do
    cat <<EOF

(rule
 (with-stdout-to dune-cache-daemon-$cmd.1
  (run %{bin:dune-cache-daemon} $cmd --help=groff)))

(install
 (section man)
 (files dune-cache-daemon-$cmd.1))
EOF
done

echo
