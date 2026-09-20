#!/usr/bin/env bash
# Assert a built image reports the version it is tagged with and carries a
# genuine ELF of the right architecture, on every architecture it carries
# (not just the host's binary running because the host happens to execute it
# natively), and that the server inside it answers `/meta` with that version
# and the `selvage/1` wire version on the runner's own architecture.
#
#   scripts/assert-image-version.sh <image-ref> <version> [port]
#
# Shared by the publish job and by the non-pushing rehearsal, so a release
# asserts the built manifest list with the same code that rehearsed it. Needs a
# Docker daemon; `--platform` is explicit throughout because a manifest list
# otherwise resolves to the host's architecture and the second half of the
# proof would quietly be the first half twice.
#
# `--version` prints a string baked into the binary at compile time, so it
# reads the same regardless of which architecture actually compiled it — a
# binary built for the wrong platform still passes that check, which is
# exactly how a mislabelled arm64 leg shipped undetected before. The ELF
# check reads the binary's own header (byte 18: 62 is x86-64, 183 is
# AArch64) instead of trusting anything the image claims about itself, and it
# is the only check run against the architecture that is not the runner's
# own: `docker create` and `docker cp` copy the binary out without executing
# it, so this needs no binfmt/qemu support on the runner. Actually running the
# image — `--version`, and the live `/meta` round-trip — stays scoped to the
# runner's own architecture, which needs no emulation either.
set -Eeuo pipefail
report_failure() {
    local msg="assert-image-version.sh failed at line $1: $2"
    echo "::error::$msg"
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        printf '### assert-image-version.sh failed\n%s\n' "$msg" >> "$GITHUB_STEP_SUMMARY"
    fi
}
trap 'report_failure "$LINENO" "$BASH_COMMAND"' ERR

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

image="${1:?usage: assert-image-version.sh <image-ref> <version> [port]}"
version="${2:?usage: assert-image-version.sh <image-ref> <version> [port]}"
port="${3:-18080}"
want="selvaged/$version"

declare -A elf_machine=([amd64]=62 [arm64]=183)
host_arch="$(docker version --format '{{.Server.Arch}}')"
echo "host arch: $host_arch"

TMPDIR="${TMPDIR:-$repo_root/.tmp}"
mkdir -p "$TMPDIR"

container_names=()
cleanup() {
    for n in "${container_names[@]}"; do
        docker rm -f "$n" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

for arch in amd64 arm64; do
    extract_name="version-elf-$arch-$PPID-$$"
    container_names+=("$extract_name")
    docker create --platform "linux/$arch" --name "$extract_name" "$image" >/dev/null
    bin="$TMPDIR/assert-image-version-$arch"
    docker cp "$extract_name:/selvaged" "$bin" >/dev/null
    docker cp "$extract_name:/build-targetarch-debug.txt" - 2>/dev/null | tar -xO 2>/dev/null | sed "s/^/[$arch] /" || true
    docker rm -f "$extract_name" >/dev/null
    magic="$(od -An -tx1 -N4 "$bin" | tr -d ' \n')"
    if [ "$magic" != "7f454c46" ]; then
        echo "linux/$arch image's /selvaged is not an ELF binary" >&2
        exit 1
    fi
    machine="$(od -An -tu1 -j18 -N1 "$bin" | tr -d ' ')"
    want_machine="${elf_machine[$arch]}"
    if [ "$machine" != "$want_machine" ]; then
        echo "linux/$arch image's /selvaged has ELF e_machine $machine, want $want_machine ($arch)" >&2
        exit 1
    fi
    echo "ELF OK on linux/$arch: e_machine $machine"
    rm -f "$bin"

    if [ "$arch" = "$host_arch" ]; then
        out="$(docker run --rm --platform "linux/$arch" "$image" --version)"
        if [ "$out" != "$want" ]; then
            echo "linux/$arch --version says '$out', want '$want'" >&2
            exit 1
        fi
        echo "version OK on linux/$arch: $out"

        name="version-smoke-$arch-$PPID-$$"
        container_names+=("$name")
        docker run --detach --name "$name" --platform "linux/$arch" \
            --publish "127.0.0.1:$port:8080" "$image"
        scripts/check-server-version.sh "http://127.0.0.1:$port" "$version" "$want"
        docker rm -f "$name" >/dev/null
    fi
done
