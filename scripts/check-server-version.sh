#!/usr/bin/env bash
# Assert a running server's version truthfulness: `--version` output and
# `GET <base-url>/meta` must both say `selvaged/<expected-version>`, and the
# meta document must advertise the `selvage/1` wire version.
#
#   check-server-version.sh <base-url> <expected-version> [version-output]
#
# The third argument is a captured `--version` stdout; when absent, only /meta
# is checked. The /meta read polls with a deadline — a server that never
# answers fails loudly instead of hanging the caller. Exit non-zero on any
# mismatch, naming what disagreed.
set -euo pipefail

base_url="${1:?usage: check-server-version.sh <base-url> <expected-version> [version-output]}"
expected_version="${2:?usage: check-server-version.sh <base-url> <expected-version> [version-output]}"
version_output="${3:-}"
expected_server="selvaged/$expected_version"

if [ -n "$version_output" ] && [ "$version_output" != "$expected_server" ]; then
    echo "--version says '$version_output', want '$expected_server'" >&2
    exit 1
fi

meta=""
for _ in $(seq 1 30); do
    if meta="$(curl -sS --fail --max-time 2 "$base_url/meta")"; then
        break
    fi
    meta=""
    sleep 1
done
if [ -z "$meta" ]; then
    echo "GET $base_url/meta never answered within the deadline" >&2
    exit 1
fi

META_JSON="$meta" EXPECTED_SERVER="$expected_server" python3 - <<'EOF'
import json
import os
import sys

meta = json.loads(os.environ["META_JSON"])
want = os.environ["EXPECTED_SERVER"]
server = meta.get("server")
wires = meta.get("wire_versions", [])
if server != want:
    print(f"/meta server is {server!r}, want {want!r}", file=sys.stderr)
    sys.exit(1)
if "selvage/1" not in wires:
    print(f"/meta wire_versions is {wires!r}, want 'selvage/1' among them",
          file=sys.stderr)
    sys.exit(1)
EOF

echo "version OK: $expected_server with selvage/1 at $base_url"
