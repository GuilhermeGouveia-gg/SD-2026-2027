#!/bin/bash
set -eu
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for role in produtor lider replica processador; do
    mkdir -p "$DIR/rep04-$role/out"
    javac --release 11 -encoding UTF-8 -Xlint:all -d "$DIR/rep04-$role/out" "$DIR/rep04-$role/src/rep04/"*.java
done
