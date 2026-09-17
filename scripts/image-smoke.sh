#!/usr/bin/env bash
# Full image smoke without publishing and without Docker. Builds the image
# with nix/dockerTools (the CI runners cannot execute buildx builds —
# established over nine red rounds — but run nix reliably), verifies the
# manifest and container config with skopeo, extracts the exact binary from
# the image layers, and asserts `--version` and `GET /meta` truthfulness
# against it. Takes the expected Go architecture (amd64/arm64; defaults to
# the build host). Used by .github/workflows/image.yml and ci-local.sh.
set -euo pipefail

want_arch="${1:-$(if [ "$(uname -m)" = "aarch64" ]; then echo arm64; else echo amd64; fi)}"
VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
SHA="$(git rev-parse --short HEAD)"
TAG="$(scripts/image-tag.sh "$SHA")"
RESULT=".tmp/nix-image-result"
# skopeo ships no signature policy by itself; the smoke copies unsigned
# local images only, so it names the upstream default policy explicitly
# (accept anything) instead of depending on host configuration.
SKOPEO_POLICY="scripts/skopeo-policy.json"
SKOPEO_LINK=".tmp/skopeo-result"
OCI_DIR=".tmp/oci-image"
BIN=".tmp/image-selvaged"

cleanup() {
    kill "$server_pid" >/dev/null 2>&1 || true
}
server_pid=""
trap cleanup EXIT
mkdir -p .tmp

nix build .#image --out-link "$RESULT" --print-build-logs

# skopeo by direct store path, not via `nix develop`: the smoke stays out
# of the interactive environment entirely (one fewer moving part per step).
nix build .#skopeo --out-link "$SKOPEO_LINK" --print-build-logs
SKOPEO="$SKOPEO_LINK/bin/skopeo"

manifest_json="$("$SKOPEO" inspect --raw "docker-archive:$RESULT")"
MANIFEST_JSON="$manifest_json" EXPECTED_ARCH="$want_arch" python3 - <<'EOF'
import json
import os
import sys

manifest = json.loads(os.environ["MANIFEST_JSON"])
want = os.environ["EXPECTED_ARCH"]
if len(manifest.get("layers", [])) < 1:
    print("image has no layers", file=sys.stderr)
    sys.exit(1)
print(f"manifest OK: {len(manifest['layers'])} layer(s), want arch {want}")
EOF

config_json="$("$SKOPEO" inspect --config "docker-archive:$RESULT")"
CONFIG_JSON="$config_json" EXPECTED_ARCH="$want_arch" EXPECTED_VERSION="$VERSION" python3 - <<'EOF'
import json
import os
import sys

failures = []


def check(what, got, want):
    if got != want:
        failures.append(f"{what} is {got!r}, want {want!r}")


config = json.loads(os.environ["CONFIG_JSON"])
check("architecture", config.get("architecture"), os.environ["EXPECTED_ARCH"])
check("os", config.get("os"), "linux")
inner = config.get("config", {})
check("User", inner.get("User"), "65532")
check("Entrypoint", inner.get("Entrypoint"), ["/bin/selvaged"])
check("Cmd", inner.get("Cmd"), ["--listen", "0.0.0.0:8080"])
check("ExposedPorts", inner.get("ExposedPorts"), {"8080/tcp": {}})
labels = inner.get("Labels", {})
check("licenses label", labels.get("org.opencontainers.image.licenses"),
      "FSL-1.1-MIT")
check("version label", labels.get("org.opencontainers.image.version"),
      os.environ["EXPECTED_VERSION"])
if failures:
    print("\n".join(failures), file=sys.stderr)
    sys.exit(1)
print("config OK: entrypoint, user, port, FSL and version labels, arch")
EOF

OCI_DIR="$OCI_DIR" python3 - <<'EOF'
import os
import shutil

oci = os.environ["OCI_DIR"]
shutil.rmtree(oci, ignore_errors=True)
print(f"extracting to a fresh {oci}")
EOF
"$SKOPEO" copy --policy "$SKOPEO_POLICY" "docker-archive:$RESULT" "oci:$OCI_DIR"
OCI_DIR="$OCI_DIR" OUT_BIN="$BIN" python3 - <<'EOF'
import json
import os
import tarfile

oci = os.environ["OCI_DIR"]
with open(os.path.join(oci, "index.json")) as f:
    index = json.load(f)
manifest_digest = index["manifests"][0]["digest"].split(":", 1)[1]
with open(os.path.join(oci, "blobs", "sha256", manifest_digest)) as f:
    manifest = json.load(f)
out = os.environ["OUT_BIN"]
for layer in manifest["layers"]:
    blob = os.path.join(oci, "blobs", "sha256",
                        layer["digest"].split(":", 1)[1])
    with tarfile.open(blob) as tar:
        try:
            member = tar.getmember("./bin/selvaged")
        except KeyError:
            continue
        with tar.extractfile(member) as src, open(out, "wb") as dest:
            dest.write(src.read())
        break
else:
    raise SystemExit("bin/selvaged not found in any layer")
print(f"extracted the image binary to {out}")
EOF
chmod +x "$BIN"

version_output="$("$BIN" --version)"
"$BIN" --listen 127.0.0.1:18080 >/dev/null 2>&1 &
server_pid=$!
scripts/check-server-version.sh http://127.0.0.1:18080 "$VERSION" "$version_output"
kill "$server_pid" >/dev/null 2>&1 || true
server_pid=""

want="ghcr.io/selvage-protocol/selvaged:$VERSION-$SHA"
if [ "$TAG" != "$want" ]; then
    echo "image tag is '$TAG', want '$want'" >&2
    exit 1
fi
echo "tag OK: $TAG (unpublished — smoke only, nothing pushed)"
