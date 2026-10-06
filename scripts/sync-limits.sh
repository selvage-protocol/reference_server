#!/usr/bin/env bash
#
# The published numeric bounds in `crates/harness/tests/limits.json` are vendored, not
# authored here: the canonical file lives in the specification repository
# (`selvage-protocol/specification`, its `schema/limits.json`), and
# `crates/harness/tests/limits.rs` reads this copy so that a plain `cargo test` and the Nix
# sandbox need no sibling checkout. Run this after a specification change:
#
#   scripts/sync-limits.sh [path-to-specification-checkout]
#
# The default source is the sibling checkout `../specification`. The script copies and then
# reports the difference, so a run either brings the file into agreement or says what it
# could not.
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${1:-"$here/../specification"}"
source_file="$source_dir/schema/limits.json"
target="$here/crates/harness/tests/limits.json"

if [[ ! -f "$source_file" ]]; then
  printf 'no limits file at %s: pass the path to a specification checkout\n' "$source_file" >&2
  exit 1
fi

cp -a "$source_file" "$target"

if diff -u "$source_file" "$target"; then
  printf 'crates/harness/tests/limits.json is the same as %s\n' "$source_file"
else
  printf 'crates/harness/tests/limits.json differs from %s\n' "$source_file" >&2
  exit 1
fi
