#!/bin/sh
# Build a dependency-only skeleton of the workspace for Docker layer caching.
#
# Usage: docker-skeleton.sh <workspace-dir> <out-dir>
#
# Copies every crate's Cargo.toml, build.rs and proto/ unchanged, and writes a
# stub for every Cargo target root that exists in the source tree
# (src/lib.rs, src/main.rs, src/bin/*, benches/*, examples/*, tests/*), so
# `cargo fetch` / `cargo build` can parse every manifest — including targets
# declared with [[bin]] / [[bench]] — without the real sources. The skeleton
# only changes when manifests, build scripts or the set of target files
# change, so the dependency layer stays cached across ordinary code edits.
set -eu

src=$1
out=$2

cd "$src"
for manifest in crates/*/Cargo.toml; do
    dir=$(dirname "$manifest")
    mkdir -p "$out/$dir"
    cp "$manifest" "$out/$dir/"
    [ -f "$dir/build.rs" ] && cp "$dir/build.rs" "$out/$dir/"
    [ -d "$dir/proto" ] && cp -R "$dir/proto" "$out/$dir/"
done

find crates \( -path 'crates/*/src/lib.rs' \
            -o -path 'crates/*/src/main.rs' \
            -o -path 'crates/*/src/bin/*.rs' \
            -o -path 'crates/*/benches/*.rs' \
            -o -path 'crates/*/examples/*.rs' \
            -o -path 'crates/*/tests/*.rs' \) -type f |
while read -r file; do
    mkdir -p "$out/$(dirname "$file")"
    case "$file" in
        */src/lib.rs) echo 'pub fn _stub() {}' ;;
        *) echo 'fn main() {}' ;;
    esac > "$out/$file"
done
