#!/usr/bin/env bash
#
# The wire vectors in `vectors/` are vendored, not authored here: the canonical set lives in
# the specification repository (`selvage-protocol/specification`, its `vectors/` directory),
# and `crates/harness/tests/vectors.rs` replays this copy so that a plain `cargo test` and the
# Nix sandbox need no sibling checkout. Run this after a specification change:
#
#   scripts/sync-vectors.sh [path-to-specification-checkout]
#
# The default source is the sibling checkout `../specification`. The script copies and then
# reports the difference, so a run either brings the directory into agreement or says what it
# could not.
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${1:-"$here/../specification"}"

if [[ ! -d "$source_dir/vectors" ]]; then
  printf 'no vectors in %s: pass the path to a specification checkout\n' "$source_dir" >&2
  exit 1
fi

cp -a "$source_dir/vectors/." "$here/vectors/"

# A vector that the specification has retired must go too, or the copy drifts by addition.
while IFS= read -r file; do
  [[ -e "$source_dir/$file" ]] || rm -- "$here/$file"
done < <(cd "$here" && find vectors -type f)
find "$here/vectors" -mindepth 1 -type d -empty -delete

if diff -r "$source_dir/vectors" "$here/vectors"; then
  printf 'vectors/ is the same as %s/vectors\n' "$source_dir"
else
  printf 'vectors/ differs from %s/vectors\n' "$source_dir" >&2
  exit 1
fi
