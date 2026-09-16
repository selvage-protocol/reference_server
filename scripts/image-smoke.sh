#!/usr/bin/env bash
# Full image smoke without publishing. Builds the multi-arch image to a local
# OCI tarball, asserts its manifest list names both architectures, then
# loads and runs each architecture: `--version` and `GET /meta` must agree
# with the Cargo version. Nothing is pushed anywhere — there is no registry
# in the loop at all, loopback or otherwise.
#
# Needs: docker, a working buildx builder that can build multi-arch (the CI
# workflow sets one up with Blacksmith's builder action; a stock install's
# docker driver cannot build multi-arch), and QEMU binfmt for the arm64 run.
# Run from the repository root.
set -euo pipefail

VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
SHA="$(git rev-parse --short HEAD)"
TAG="$(scripts/image-tag.sh "$SHA")"
TARBALL=".tmp/selvaged-smoke.tar"

cleanup() {
    docker rm -f smoke-amd64 smoke-arm64 >/dev/null 2>&1 || true
}
trap cleanup EXIT
mkdir -p .tmp

if ! docker buildx inspect --bootstrap >/dev/null 2>&1; then
    echo "no working buildx builder (needed for the multi-arch smoke)" >&2
    docker buildx ls || true
    exit 1
fi

# One multi-arch build to a local file. `imagetools inspect` only speaks to
# registries, so the manifest list is asserted from the tarball's index
# instead — that index IS the multi-arch manifest list.
docker buildx build --platform linux/amd64,linux/arm64 \
    -o "type=oci,dest=$TARBALL" \
    --build-arg "VERSION=$VERSION" \
    --build-arg "REVISION=$(git rev-parse HEAD)" .
TARBALL_PATH="$TARBALL" python3 - <<'EOF'
import json
import os
import tarfile

path = os.environ["TARBALL_PATH"]
with tarfile.open(path) as tar:
    index = json.load(tar.extractfile("index.json"))
arches = sorted(
    m["platform"]["architecture"] for m in index["manifests"]
)
if arches != ["amd64", "arm64"]:
    print(f"manifest arches are {arches}, want ['amd64', 'arm64']")
    raise SystemExit(1)
print(f"manifest OK: {arches}")
EOF

port_for() {
    if [ "$1" = "amd64" ]; then echo 18080; else echo 18081; fi
}

for arch in amd64 arm64; do
    port="$(port_for "$arch")"
    docker buildx build --platform "linux/$arch" --load \
        -t "selvaged:smoke-$arch" \
        --build-arg "VERSION=$VERSION" \
        --build-arg "REVISION=$SHA" .
    version_output="$(docker run --rm --platform "linux/$arch" \
        "selvaged:smoke-$arch" --version)"
    docker run -d --name "smoke-$arch" --platform "linux/$arch" \
        -p "127.0.0.1:$port:8080" "selvaged:smoke-$arch" >/dev/null
    scripts/check-server-version.sh "http://127.0.0.1:$port" \
        "$VERSION" "$version_output"
    docker rm -f "smoke-$arch" >/dev/null
done

want="ghcr.io/selvage-protocol/selvaged:$VERSION-$SHA"
if [ "$TAG" != "$want" ]; then
    echo "image tag is '$TAG', want '$want'" >&2
    exit 1
fi
echo "tag OK: $TAG (unpublished — smoke only, nothing pushed)"
