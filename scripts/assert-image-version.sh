#!/usr/bin/env bash
# Assert a built image reports the version it is tagged with, on every
# architecture it carries, and that the server inside it answers `/meta` with
# that version and the `selvage/1` wire version.
#
#   scripts/assert-image-version.sh <image-ref> <version> [port]
#
# Shared by the publish job and by the non-pushing rehearsal, so a release
# asserts the built manifest list with the same code that rehearsed it. Needs a
# Docker daemon and, for the architecture that is not the host's, the runner's
# binfmt registrations (`docker/setup-qemu-action`): `--platform` is explicit
# because a manifest list otherwise resolves to the host's architecture and the
# second half of the proof would quietly be the first half twice.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

image="${1:?usage: assert-image-version.sh <image-ref> <version> [port]}"
version="${2:?usage: assert-image-version.sh <image-ref> <version> [port]}"
port="${3:-18080}"
want="selvaged/$version"

for arch in amd64 arm64; do
    out="$(docker run --rm --platform "linux/$arch" "$image" --version)"
    if [ "$out" != "$want" ]; then
        echo "linux/$arch --version says '$out', want '$want'" >&2
        exit 1
    fi
    echo "version OK on linux/$arch: $out"
done

name="version-smoke-$PPID-$$"
cleanup() {
    docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --name "$name" --platform linux/amd64 \
    --publish "127.0.0.1:$port:8080" "$image"
scripts/check-server-version.sh "http://127.0.0.1:$port" "$version" "$want"
