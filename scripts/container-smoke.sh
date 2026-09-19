#!/usr/bin/env bash
# The container smoke: build the image with the repository's own Dockerfile, run
# it under the hardening the hosting gate requires, and join a real room over
# the WebSocket with the harness's client engine.
#
#   scripts/container-smoke.sh [PORT]
#
# Needs a Docker daemon with the compose plugin and the dev shell's cargo (the
# join is a real client, not a hand-written frame): `.github/workflows/image.yml`
# runs it on a runner that has all of them. Proves, in order: `docker build`
# succeeds; `compose.yaml` carries the hardened run a self-hoster gets; the
# container actually runs that way (read-only root filesystem, every capability
# dropped, no-new-privileges, nothing mounted); the container's own binary and
# `/meta` report the version and wire version `Cargo.toml` names; the page baked
# into the image is served with the headers the static handler pins, a
# content-hashed name being immutable and a stable one revalidating; the client
# engine mints a room in that container, joins it as a guest, and converges on
# an edit over the container's socket; and a mounted page still overrides the
# baked one.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

# `/tmp` is RAM-backed on some hosts, and a build there has taken one down before:
# keep the scratch, the page and the cargo target inside the checkout.
export TMPDIR="$repo_root/.tmp"
mkdir -p "$TMPDIR"

for tool in docker nix curl python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "container-smoke.sh needs $tool, which this host does not have" >&2
    exit 2
  fi
done
if ! docker compose version >/dev/null 2>&1; then
  echo "container-smoke.sh needs the docker compose plugin to read compose.yaml" >&2
  exit 2
fi

port="${1:-18080}"
image="selvaged-smoke"
# Named per invocation: a `SIGKILL`ed or crashed run leaves its container behind,
# and a second invocation must not collide with it — or remove its container.
name="selvaged-smoke-$PPID-$$"
page_dir="$TMPDIR/container-smoke-page"
report_dir="$TMPDIR/container-smoke"
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
# The hardening the container run must carry: read-only root filesystem, every
# capability dropped, and no process able to gain one.
hardening=(--read-only --cap-drop ALL --security-opt no-new-privileges:true)

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

echo "=== compose: the hardened run is in the file a self-hoster uses, not only here ==="
compose_json="$(docker compose -f compose.yaml config --format json)"
COMPOSE_JSON="$compose_json" python3 - <<'EOF'
import json
import os
import sys

service = json.loads(os.environ["COMPOSE_JSON"])["services"]["selvaged"]
failures = []

if service.get("read_only") is not True:
    failures.append(f"read_only is {service.get('read_only')!r}, want true")
if service.get("cap_drop") != ["ALL"]:
    failures.append(f"cap_drop is {service.get('cap_drop')!r}, want ['ALL']")
if not any(
    opt.startswith("no-new-privileges")
    for opt in service.get("security_opt", [])
):
    failures.append(
        f"security_opt is {service.get('security_opt')!r}, "
        "want no-new-privileges among it"
    )
# Nothing mounted: no host socket — a Docker socket above all — and no writable
# path, because the image carries the page and the server keeps nothing on disk.
if service.get("volumes"):
    failures.append(
        f"volumes is {service['volumes']!r}, want none: nothing on the host is "
        "bound into a public deployment's container"
    )
if service.get("privileged"):
    failures.append("privileged is set, want it unset")
for key in ("network_mode", "pid", "ipc"):
    if service.get(key) == "host":
        failures.append(f"{key} is host, want the container's own")

if failures:
    print("\n".join(failures), file=sys.stderr)
    sys.exit(1)
print(
    "compose OK: read-only root filesystem, all capabilities dropped, "
    "no-new-privileges, nothing mounted"
)
EOF

echo "=== build: docker build of the repository's Dockerfile ==="
docker build --tag "$image" .

echo "=== run: the hardened container, its own command, the page baked in ==="
docker run --detach --name "$name" "${hardening[@]}" \
  --publish "127.0.0.1:$port:8080" \
  "$image"

# The flags were passed; this asserts the daemon took them and that nothing
# else came along — a run that silently fell back to a writable root or to the
# host's namespaces would otherwise pass every HTTP assertion below.
inspect_json="$(docker inspect "$name")"
INSPECT_JSON="$inspect_json" python3 - <<'EOF'
import json
import os
import sys

container = json.loads(os.environ["INSPECT_JSON"])[0]
host = container["HostConfig"]
failures = []

if host.get("ReadonlyRootfs") is not True:
    failures.append(f"ReadonlyRootfs is {host.get('ReadonlyRootfs')!r}, want true")
if host.get("CapDrop") != ["ALL"]:
    failures.append(f"CapDrop is {host.get('CapDrop')!r}, want ['ALL']")
if host.get("CapAdd"):
    failures.append(f"CapAdd is {host['CapAdd']!r}, want none")
if not any(
    opt.startswith("no-new-privileges")
    for opt in host.get("SecurityOpt") or []
):
    failures.append(
        f"SecurityOpt is {host.get('SecurityOpt')!r}, want no-new-privileges among it"
    )
if host.get("Privileged"):
    failures.append("the container is privileged, want it not")
if container.get("Mounts"):
    failures.append(
        f"the container has {container['Mounts']!r}, want no mount at all"
    )

if failures:
    print("\n".join(failures), file=sys.stderr)
    sys.exit(1)
print(
    "run OK: read-only root filesystem, all capabilities dropped, "
    "no-new-privileges, no mount"
)
EOF

# The container's binary must report the version the manifest names, and `/meta`
# must agree, wire version included. The `/meta` read polls with a deadline.
image_version="$(docker run --rm "${hardening[@]}" "$image" --version)"
scripts/check-server-version.sh "http://127.0.0.1:$port" "$version" "$image_version"

echo "=== page: the page baked into the image, served with no mount ==="
base="http://127.0.0.1:$port"
baked="$report_dir/baked"
mkdir -p "$baked"
curl -sS --fail --max-time 10 -D "$baked/index.headers" \
  -o "$baked/index.html" "$base/"
curl -sS --fail --max-time 10 -D "$baked/app.headers" \
  -o "$baked/app.js" "$base/app.js"
curl -sS --fail --max-time 10 -D "$baked/manifest.headers" \
  -o "$baked/site.webmanifest" "$base/site.webmanifest"

require_header "$baked/index.headers" "HTTP/1.1 200 OK"
require_header "$baked/index.headers" "content-type: text/html; charset=utf-8"
require_header "$baked/index.headers" "cache-control: no-cache"
require_header "$baked/index.headers" "referrer-policy: no-referrer"
require_header "$baked/index.headers" "x-content-type-options: nosniff"
require_header "$baked/index.headers" "content-security-policy: default-src 'none'"
require_body "$baked/index.html" "<title>Selvage"
require_header "$baked/app.headers" "content-type: text/javascript; charset=utf-8"
require_header "$baked/app.headers" "cache-control: no-cache"
require_body "$baked/app.js" "selvage/1"
require_header "$baked/manifest.headers" "content-type: application/json; charset=utf-8"

# A content-hashed chunk, named by the bundle the image carries rather than by
# this script: its name is the hash of its bytes, so it can never change under
# that name and the policy may pin it. An extraction that found nothing would
# otherwise report the page clean without having read it.
chunk="$(grep -o 'lang-[A-Za-z0-9_-]\{8,\}\.js' "$baked/app.js" | head -n 1)"
if [ -z "$chunk" ]; then
  echo "the baked bundle names no content-hashed chunk, so it is not the bundler's output" >&2
  exit 1
fi
curl -sS --fail --max-time 10 -D "$baked/chunk.headers" -o "$baked/chunk.js" "$base/$chunk"
require_header "$baked/chunk.headers" "content-type: text/javascript; charset=utf-8"
require_header "$baked/chunk.headers" "cache-control: public, max-age=31536000, immutable"
echo "baked page OK: $chunk served from the image, typed, cached by name and hardened"

echo "=== join: a client engine mints a room in the container and joins it ==="
nix develop . -c cargo run --quiet -p selvage-harness --example join_room \
  -- "ws://127.0.0.1:${port}"

echo "=== the container's own transcript ==="
docker logs "$name"

echo "=== override: a mounted page replaces the baked one ==="
# No command override: the image's own command already serves `--serve-page
# /page`, so the mount is the whole override — the same one compose.yaml
# documents for a page built elsewhere.
docker rm -f "$name" >/dev/null
docker run --detach --name "$name" "${hardening[@]}" \
  --publish "127.0.0.1:$port:8080" \
  --volume "$page_dir:/page:ro" \
  "$image"

override="$report_dir/override"
mkdir -p "$override"
curl -sS --fail --max-time 10 -D "$override/index.headers" \
  -o "$override/index.html" "$base/"
curl -sS --fail --max-time 10 -D "$override/hashed.headers" \
  -o "$override/hashed.js" "$base/app-1a2b3c4d.js"

require_header "$override/index.headers" "HTTP/1.1 200 OK"
require_header "$override/index.headers" "content-type: text/html; charset=utf-8"
require_header "$override/index.headers" "cache-control: no-cache"
require_header "$override/index.headers" "referrer-policy: no-referrer"
require_header "$override/index.headers" "x-content-type-options: nosniff"
require_header "$override/index.headers" "content-security-policy: default-src 'none'"
require_body "$override/index.html" "selvage container smoke"
require_header "$override/hashed.headers" "content-type: text/javascript; charset=utf-8"
require_header "$override/hashed.headers" "cache-control: public, max-age=31536000, immutable"
require_header "$override/hashed.headers" "content-security-policy: default-src 'none'"
require_body "$override/hashed.js" "export const smoke = 1;"
echo "override OK: the mount replaced the baked page, typed and cached the same way"

echo "container smoke OK: $image built, ran hardened, served its baked page and a mounted one, and joined a room"
