#!/usr/bin/env bash
# The image's release identity, in the one place both jobs that need it read
# it: the non-pushing rehearsal and the publish job.
#
#   scripts/release-tags.sh [REF_NAME]
#
# Prints `version=`, `sha=` and `name=` lines, the shape `$GITHUB_OUTPUT`
# wants. The version comes from Cargo.toml and the sha from git, so the tag and
# the binary inside it name the same revision. A `REF_NAME` that is a version
# tag must agree with Cargo.toml: a tag cut from the wrong commit would
# otherwise publish a mismatched version. A ref that is not one (a pull
# request's branch) has no version to agree with, so nothing is checked there —
# the tag run is where that rule is enforced.
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
esac

printf 'version=%s\nsha=%s\nname=%s-%s\n' "$version" "$sha" "$version" "$sha"
