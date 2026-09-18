#!/usr/bin/env bash
# The container smoke: build the image with the repository's own Dockerfile, run
# it with a page mounted, and join a real room over the WebSocket with the
# harness's client engine.
#
#   scripts/container-smoke.sh [PORT]
#
# Needs a Docker daemon and the dev shell's cargo (the join is a real client, not
# a hand-written frame): `.github/workflows/image.yml` runs it on a runner that
# has both. Proves, in order: `docker build` succeeds; the container's own binary
# and `/meta` report the version and wire version `Cargo.toml` names; `GET /`
# serves the mounted page with the headers the static handler pins, a
# content-hashed name being immutable and a stable one revalidating; and the
# client engine mints a room in that container, joins it as a guest, and
# converges on an edit over the container's socket.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

# `/tmp` is RAM-backed on some hosts, and a build there has taken one down before:
# keep the scratch, the page and the cargo target inside the checkout.
export TMPDIR="$repo_root/.tmp"
mkdir -p "$TMPDIR"

for tool in docker nix curl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "container-smoke.sh needs $tool, which this host does not have" >&2
    exit 2
  fi
done

port="${1:-18080}"
image="selvaged-smoke"
# Named per invocation: a `SIGKILL`ed or crashed run leaves its container behind,
# and a second invocation must not collide with it — or remove its container.
name="selvaged-smoke-$PPID-$$"
page_dir="$TMPDIR/container-smoke-page"
report_dir="$TMPDIR/container-smoke"
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The page in a deployment is the browser client's `dist/`; three files are what
# the handler's decisions need. The hashed name is one the bundler writes
# (`lang-<hash>.js`), so the cache policy is proved rather than assumed.
rm -rf "$page_dir" "$report_dir"
mkdir -p "$page_dir" "$report_dir"
printf '<!doctype html><title>selvage container smoke</title>\n' >"$page_dir/index.html"
printf 'export const smoke = 1;\n' >"$page_dir/app-1a2b3c4d.js"

echo "=== build: docker build of the repository's Dockerfile ==="
docker build --tag "$image" .

echo "=== run: the container serving /meta, /session and the page on one port ==="
docker run --detach --name "$name" \
  --publish "127.0.0.1:$port:8080" \
  --volume "$page_dir:/page:ro" \
  "$image" --listen 0.0.0.0:8080 --serve-page /page

# The container's binary must report the version the manifest names, and `/meta`
# must agree, wire version included. The `/meta` read polls with a deadline.
image_version="$(docker run --rm "$image" --version)"
scripts/check-server-version.sh "http://127.0.0.1:$port" "$version" "$image_version"

echo "=== page: the headers and the bytes the static handler pins ==="
base="http://127.0.0.1:$port"
curl -sS --fail --max-time 10 -D "$report_dir/index.headers" \
  -o "$report_dir/index.html" "$base/"
curl -sS --fail --max-time 10 -D "$report_dir/hashed.headers" \
  -o "$report_dir/hashed.js" "$base/app-1a2b3c4d.js"

require_header() {
  local file="$1" want="$2"
  if ! grep -qiF -- "$want" "$file"; then
    cat "$file" >&2
    echo "the response is missing '$want'" >&2
    exit 1
  fi
}

require_body() {
  local file="$1" want="$2"
  if ! grep -qF -- "$want" "$file"; then
    cat "$file" >&2
    echo "the body is missing '$want'" >&2
    exit 1
  fi
}

require_header "$report_dir/index.headers" "HTTP/1.1 200 OK"
require_header "$report_dir/index.headers" "content-type: text/html; charset=utf-8"
require_header "$report_dir/index.headers" "cache-control: no-cache"
require_header "$report_dir/index.headers" "referrer-policy: no-referrer"
require_header "$report_dir/index.headers" "x-content-type-options: nosniff"
require_header "$report_dir/index.headers" "content-security-policy: default-src 'none'"
require_body "$report_dir/index.html" "selvage container smoke"

require_header "$report_dir/hashed.headers" "content-type: text/javascript; charset=utf-8"
require_header "$report_dir/hashed.headers" "cache-control: public, max-age=31536000, immutable"
require_header "$report_dir/hashed.headers" "content-security-policy: default-src 'none'"
require_body "$report_dir/hashed.js" "export const smoke = 1;"
echo "page OK: served from the container, typed, cached by name and hardened"

echo "=== join: a client engine mints a room in the container and joins it ==="
nix develop . -c cargo run --quiet -p selvage-harness --example join_room \
  -- "ws://127.0.0.1:$port"

echo "=== the container's own transcript ==="
docker logs "$name"

echo "container smoke OK: $image built, ran, served the page and joined a room"
