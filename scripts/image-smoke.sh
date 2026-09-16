#!/usr/bin/env bash
# Full image smoke without publishing. Builds the multi-arch image, pushes it
# to a throwaway registry on loopback, verifies the manifest list with
# `buildx imagetools inspect`, then asserts per-arch `--version` and
# `GET /meta` truthfulness. Nothing leaves the machine: the only registry
# involved listens on 127.0.0.1 and is removed on exit.
#
# Needs: docker, a working buildx builder that can build multi-arch (the CI
# workflow sets one up with Blacksmith's builder action; a stock install's
# docker driver cannot build multi-arch), and QEMU binfmt for the arm64 run.
# Run from the repository root.
set -euo pipefail

VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
SHA="$(git rev-parse --short HEAD)"
TAG="$(scripts/image-tag.sh "$SHA")"
IMAGE="127.0.0.1:5000/selvaged:ci"

cleanup() {
    docker rm -f smoke-amd64 smoke-arm64 ci-registry >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker buildx inspect --bootstrap >/dev/null 2>&1; then
    echo "no working buildx builder (needed for the multi-arch smoke)" >&2
    docker buildx ls || true
    exit 1
fi

docker run -d --name ci-registry -p 127.0.0.1:5000:5000 registry:2 >/dev/null
docker buildx build --platform linux/amd64,linux/arm64 --push -t "$IMAGE" \
    --build-arg "VERSION=$VERSION" \
    --build-arg "REVISION=$(git rev-parse HEAD)" .

echo "--- manifest list ---"
docker buildx imagetools inspect "$IMAGE"
manifest_json="$(docker buildx imagetools inspect "$IMAGE" --format '{{json .Manifest}}')"
IMAGE_MANIFEST_JSON="$manifest_json" python3 - <<'EOF'
import json
import os
import sys

manifest = json.loads(os.environ["IMAGE_MANIFEST_JSON"])
arches = sorted(
    m["platform"]["architecture"] for m in manifest["manifests"]
)
if arches != ["amd64", "arm64"]:
    print(f"manifest arches are {arches}, want ['amd64', 'arm64']",
          file=sys.stderr)
    sys.exit(1)
print(f"manifest OK: {arches}")
EOF

port_for() {
    if [ "$1" = "amd64" ]; then echo 18080; else echo 18081; fi
}

for arch in amd64 arm64; do
    port="$(port_for "$arch")"
    version_output="$(docker run --rm --platform "linux/$arch" "$IMAGE" --version)"
    docker run -d --name "smoke-$arch" --platform "linux/$arch" \
        -p "127.0.0.1:$port:8080" "$IMAGE" >/dev/null
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
