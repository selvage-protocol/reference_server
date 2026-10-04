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
# succeeds; `compose.yaml` carries the hardened two-container run a self-hoster
# gets; the container actually runs that way (read-only root filesystem, every
# capability dropped, no-new-privileges, nothing mounted); the container's own
# binary and `/meta` report the version and wire version `Cargo.toml` names; the
# image carries no page, so its root answers `404`; the client engine mints a room
# in that container, joins it as a guest, and converges on an edit over the
# container's socket; and a page directory mounted over the server's page root
# and handed to `--serve-page` is served with the headers the static handler
# pins, a content-hashed name being immutable and a stable one revalidating.
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

services = json.loads(os.environ["COMPOSE_JSON"])["services"]
failures = []

if set(services) != {"selvaged", "selvage-web"}:
    failures.append(
        f"services are {sorted(services)!r}, want the server and the page"
    )

for name, service in sorted(services.items()):
    if service.get("read_only") is not True:
        failures.append(f"{name}: read_only is {service.get('read_only')!r}, want true")
    if service.get("cap_drop") != ["ALL"]:
        failures.append(f"{name}: cap_drop is {service.get('cap_drop')!r}, want ['ALL']")
    if not any(
        opt.startswith("no-new-privileges")
        for opt in service.get("security_opt", [])
    ):
        failures.append(
            f"{name}: security_opt is {service.get('security_opt')!r}, "
            "want no-new-privileges among it"
        )
    # Nothing mounted: no host socket — a Docker socket above all — and no writable
    # path, because the server keeps nothing on disk and the page travels in its
    # own image.
    if service.get("volumes"):
        failures.append(
            f"{name}: volumes is {service['volumes']!r}, want none: nothing on the "
            "host is bound into a public deployment's container"
        )
    if service.get("privileged"):
        failures.append(f"{name}: privileged is set, want it unset")
    for key in ("network_mode", "pid", "ipc"):
        if service.get(key) == "host":
            failures.append(f"{name}: {key} is host, want the container's own")

# The page is the one published port, on loopback: the server is reached from the
# page's container and from nowhere else. A self-hoster who wants a client to dial
# the server directly adds that port deliberately.
if services.get("selvaged", {}).get("ports"):
    failures.append(f"selvaged publishes {services['selvaged']['ports']!r}, want none")
published = [
    (str(port.get("published")), port.get("target"), port.get("host_ip"))
    for port in services.get("selvage-web", {}).get("ports") or []
]
if published != [("8080", 8080, "127.0.0.1")]:
    failures.append(
        f"selvage-web publishes {published!r}, want 8080 on 127.0.0.1 and nothing else"
    )

if failures:
    print("\n".join(failures), file=sys.stderr)
    sys.exit(1)
print(
    "compose OK: both services read-only, all capabilities dropped, "
    "no-new-privileges, nothing mounted; only the page publishes a port, on loopback"
)
EOF

echo "=== build: docker build of the repository's Dockerfile ==="
docker build --tag "$image" .

echo "=== run: the hardened container, its own command ==="
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

# No page in the image: the root answers 404, and only `/meta` and `/session` are
# served. A page is a directory an operator mounts and names with `--serve-page`,
# which the override below exercises.
base="http://127.0.0.1:$port"
root_status="$(curl -sS -o /dev/null --max-time 10 -w '%{http_code}' "$base/")"
if [ "$root_status" != 404 ]; then
  echo "the image's own root answered $root_status, want 404: the image carries no page" >&2
  exit 1
fi
echo "no page OK: the image's own root is 404"

echo "=== join: a client engine mints a room in the container and joins it ==="
nix develop . -c cargo run --quiet -p selvage-harness --example join_room \
  -- "ws://127.0.0.1:${port}"

echo "=== the container's own transcript ==="
docker logs "$name"

echo "=== override: a mounted page, handed to --serve-page ==="
# The image's own command names no page, so this run replaces it:
# `--serve-page` is what the reference server keeps for an operator who supplies
# a page directory.
docker rm -f "$name" >/dev/null
docker run --detach --name "$name" "${hardening[@]}" \
  --publish "127.0.0.1:$port:8080" \
  --volume "$page_dir:/page:ro" \
  "$image" --listen 0.0.0.0:8080 --serve-page /page

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
echo "override OK: --serve-page served the mounted directory, typed and cached the same way"

echo "container smoke OK: $image built, ran hardened, answered /meta and joined a room, and served a mounted page under --serve-page"
