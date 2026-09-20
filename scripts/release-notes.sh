#!/usr/bin/env bash
# Print the Markdown notes for a release tag's GitHub Release: the version the
# tag names, the commit it points at, and the container image tags the publish
# job pushed for it. Run from the repository root.
#
#   scripts/release-notes.sh [REF_NAME]
set -euo pipefail

ref="${1:-}"
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n 1)"
sha="$(git rev-parse --short HEAD)"

case "$ref" in
    v*)
        if [ "$ref" != "v$version" ]; then
            echo "tag $ref does not match Cargo version $version" >&2
            exit 1
        fi
        ;;
    *)
        echo "ref '$ref' is not a version tag" >&2
        exit 1
        ;;
esac

cat <<EOF
selvaged $version ($sha).

Container image \`ghcr.io/selvage-protocol/selvaged\`, multi-arch
(linux/amd64, linux/arm64), published by the \`publish\` job, which pulled
both architectures back out of the registry and checked \`--version\` and
\`/meta\` against \`$version\`:

- \`ghcr.io/selvage-protocol/selvaged:$version-$sha\`
- \`ghcr.io/selvage-protocol/selvaged:$version\`
- \`ghcr.io/selvage-protocol/selvaged:latest\`

\`\`\`sh
docker run --rm -p 127.0.0.1:8080:8080 ghcr.io/selvage-protocol/selvaged:$version
\`\`\`
EOF
