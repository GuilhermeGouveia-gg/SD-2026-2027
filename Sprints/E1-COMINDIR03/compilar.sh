#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
for proj in rep03-produtor rep03-lider rep03-replica rep03-processador; do
    echo "A compilar $proj..."
    mkdir -p "$DIR/$proj/out"
    javac --release 11 -encoding UTF-8 -Xlint:all -d "$DIR/$proj/out" "$DIR/$proj/src/rep03"/*.java
    echo "OK: $proj"
done
