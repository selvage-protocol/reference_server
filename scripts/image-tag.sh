#!/usr/bin/env bash
# Print the image tag `<cargo-version>-<short-sha>` for the selvaged image,
# e.g. ghcr.io/selvage-protocol/selvaged:0.1.0-e617d81. Run from the
# repository root. The tag and the binary inside it come from the same source
# revision: the version from Cargo.toml, the sha from git.
set -euo pipefail

sha="${1:-$(git rev-parse --short HEAD)}"
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
printf 'ghcr.io/selvage-protocol/selvaged:%s-%s\n' "$version" "$sha"
