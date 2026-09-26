#!/usr/bin/env bash
# The image's release identity, in the one place both jobs that need it read
# it: the non-pushing rehearsal and the publish job.
#
#   scripts/release-tags.sh [REF_NAME]
#
# Prints `version=`, `sha=`, `name=` and `latest=` lines, the shape
# `$GITHUB_OUTPUT` wants. The version comes from Cargo.toml and the sha from
# git, so the tag and the binary inside it name the same revision. A `REF_NAME`
# that is a version tag must agree with Cargo.toml: a tag cut from the wrong
# commit would otherwise publish a mismatched version. A ref that is not one (a
# pull request's branch) has no version to agree with, so nothing is checked
# there — the tag run is where that rule is enforced.
#
# `latest=false` for a prerelease, a version with a `-` suffix (`1.0.0-rc.1`):
# the public demo follows `latest` without an approval, so only a release moves
# it.
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

case "$version" in
    *-*) latest=false ;;
    *) latest=true ;;
esac

printf 'version=%s\nsha=%s\nname=%s-%s\nlatest=%s\n' "$version" "$sha" "$version" "$sha" "$latest"
