#!/usr/bin/env bash
#
# Move the two values a release carries that a tag is not: the version every
# manifest agrees on, and the `web_client` revision the server image bakes into
# the page it serves beside its binary.
#
#   scripts/bump-version.sh <X.Y.Z> --page-sha <40 lowercase hex>
#
# The caller — the release coordinator, which computes the next version — commits
# what this changes. This script never commits, tags or pushes, and it writes
# three files and no others:
#
#   Cargo.toml   `[workspace.package] version`, the string a release tag is
#                asserted against (`scripts/release-tags.sh`). Which string each
#                artefact keeps is `docs/runbook-release.md` §1's table.
#   Cargo.lock   regenerated with cargo (`update --workspace`), never edited by
#                hand: the four local packages carry the workspace version, and a
#                lock that disagrees with the manifest fails the build.
#   Dockerfile   `ARG WEB_CLIENT_SHA=`, the revision the page stage clones and the
#                value it records in `com.selvage.page.revision`.
#
# Everything else that names a version in prose — `.env.example`'s example tag,
# a workflow's dispatch description — is an example, not a home for the value.
#
# `--page-sha` is required even when it does not move: a release that built a page
# has a revision, and a caller that omits it would leave the image serving the
# previous one. A tree that already carries both values is a no-op and exit 0, not
# an error, because the coordinator may run this against a version it has already
# bumped.
#
# Refusals happen before anything is written, and any later failure restores what
# it had touched, so a failed run leaves a tree that is the tree it started from
# rather than a half-bumped one for someone to commit.
set -euo pipefail

usage='usage: scripts/bump-version.sh <X.Y.Z> --page-sha <40 lowercase hex>'

refuse() {
    printf 'refused: %s\n%s\n' "$*" "$usage" >&2
    exit 2
}

fail() {
    printf 'failed: %s\n' "$*" >&2
    exit 1
}

version=''
page_sha=''
while [ $# -gt 0 ]; do
    case "$1" in
        --page-sha)
            [ $# -ge 2 ] || refuse '--page-sha needs a value'
            [ -z "$page_sha" ] || refuse '--page-sha is given twice'
            page_sha=$2
            shift 2
            ;;
        --*)
            refuse "unknown option $1"
            ;;
        *)
            [ -z "$version" ] || refuse "more than one version given: $version and $1"
            version=$1
            shift
            ;;
    esac
done

[ -n "$version" ] || refuse 'no version given'
# The one grammar a tag can carry: no leading `v`, three numbers, and no
# prerelease or build-metadata suffix, which the tag scheme spells separately.
[[ $version =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    refuse "not a MAJOR.MINOR.PATCH version: $version"
[ -n "$page_sha" ] || refuse '--page-sha is required: the web_client commit the server image bakes'
[[ $page_sha =~ ^[0-9a-f]{40}$ ]] || refuse "not 40 lowercase hex characters: $page_sha"

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

for file in Cargo.toml Cargo.lock Dockerfile; do
    [ -f "$file" ] || fail "$repo_root/$file is not there"
done

read_manifest_version() {
    local found
    found=$(grep -c '^version = "' Cargo.toml || true)
    [ "$found" = 1 ] ||
        fail "Cargo.toml has $found top-level version lines; want exactly one"
    sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml
}

read_page_sha() {
    local found
    found=$(grep -c '^ARG WEB_CLIENT_SHA=' Dockerfile || true)
    [ "$found" = 1 ] ||
        fail "Dockerfile has $found ARG WEB_CLIENT_SHA= lines; want exactly one"
    sed -n 's/^ARG WEB_CLIENT_SHA=\(.*\)$/\1/p' Dockerfile
}

# `name version` for every local package the lock carries. Cargo writes no
# `source` for a path package and one for every registry package, so the blocks
# without one are this workspace's four crates.
lock_members() {
    awk '
        function flush() {
            if (name != "" && !sourced) printf "%s %s\n", name, version
            name = ""; version = ""; sourced = 0
        }
        /^\[\[package\]\]$/ { flush(); next }
        /^name = "/ { if (name == "") { name = $3; gsub(/"/, "", name) } }
        /^version = "/ { if (version == "") { version = $3; gsub(/"/, "", version) } }
        /^source = / { sourced = 1 }
        END { flush() }
    ' Cargo.lock
}

lock_is_current() {
    local members member member_version
    members=$(lock_members)
    [ -n "$members" ] || fail 'Cargo.lock carries no local package; it is not this workspace'"'"'s lock'
    while read -r member member_version; do
        [ "$member_version" = "$version" ] ||
            { printf '%s is %s, not %s\n' "$member" "$member_version" "$version" >&2; return 1; }
    done <<<"$members"
}

run_cargo() {
    if command -v cargo >/dev/null 2>&1; then
        cargo "$@"
    elif command -v nix >/dev/null 2>&1; then
        nix develop "$repo_root" -c cargo "$@"
    else
        printf 'no cargo on PATH and no nix to reach the flake'"'"'s toolchain\n' >&2
        return 127
    fi
}

set_manifest_version() {
    local temporary="$work/Cargo.toml.new"
    cp -p Cargo.toml "$temporary"
    awk -v new="$1" '
        /^\[/ { section = $0 }
        section == "[workspace.package]" && /^version = / {
            print "version = \"" new "\""; found = 1; next
        }
        { print }
        END { if (!found) exit 3 }
    ' Cargo.toml >"$temporary" || return 1
    mv "$temporary" Cargo.toml
}

set_page_sha() {
    local temporary="$work/Dockerfile.new"
    cp -p Dockerfile "$temporary"
    awk -v new="$1" '
        /^ARG WEB_CLIENT_SHA=/ { print "ARG WEB_CLIENT_SHA=" new; found += 1; next }
        { print }
        END { if (found != 1) exit 3 }
    ' Dockerfile >"$temporary" || return 1
    mv "$temporary" Dockerfile
}

manifest_version=$(read_manifest_version)
current_sha=$(read_page_sha)
members_stale=0
lock_is_current || members_stale=1

if [ "$manifest_version" = "$version" ] && [ "$current_sha" = "$page_sha" ] && [ "$members_stale" = 0 ]; then
    printf 'already at %s with page-sha %s; nothing changed\n' "$version" "$page_sha"
    exit 0
fi

# The originals, so a cargo that refuses or a Dockerfile that has lost its one
# `ARG WEB_CLIENT_SHA=` line ends with the tree it began with. Inside the
# checkout: nowhere here writes to a system temporary directory.
work="$repo_root/.tmp/bump-version.$$"
mkdir -p "$work"
restore() {
    cp -p "$work/Cargo.toml" Cargo.toml
    cp -p "$work/Cargo.lock" Cargo.lock
    cp -p "$work/Dockerfile" Dockerfile
}
# The `.tmp` directory goes too when it was made for this run and nothing else
# is in it, so a bumped tree is the tree it was plus the three files.
trap 'rm -rf "$work"; rmdir "$repo_root/.tmp" 2>/dev/null || true' EXIT
cp -p Cargo.toml Cargo.lock Dockerfile "$work/"

touch_manifest=0
touch_lock=0
touch_page=0
[ "$manifest_version" = "$version" ] || touch_manifest=1
[ "$members_stale" = 0 ] || touch_lock=1
[ "$current_sha" = "$page_sha" ] || touch_page=1

if [ "$touch_manifest" = 1 ] || [ "$touch_lock" = 1 ]; then
    if [ "$touch_manifest" = 1 ]; then
        set_manifest_version "$version" || { restore; fail 'Cargo.toml has no [workspace.package] version line'; }
    fi
    if ! run_cargo update --workspace; then
        restore
        fail 'cargo update --workspace failed; Cargo.toml and Cargo.lock are as they were'
    fi
fi

if [ "$touch_page" = 1 ]; then
    set_page_sha "$page_sha" || { restore; fail 'Dockerfile has no one ARG WEB_CLIENT_SHA= line to set'; }
fi

if [ "$(read_manifest_version)" != "$version" ]; then
    restore
    fail "Cargo.toml still does not carry $version; the tree is as it was"
fi
if [ "$(read_page_sha)" != "$page_sha" ]; then
    restore
    fail "Dockerfile still does not carry $page_sha; the tree is as it was"
fi
if ! lock_is_current; then
    restore
    fail "Cargo.lock does not carry $version for every local package; the tree is as it was"
fi

printf 'version %s -> %s\n' "$manifest_version" "$version"
printf 'page-sha %s -> %s\n' "$current_sha" "$page_sha"
[ "$touch_manifest" = 0 ] || printf 'changed: Cargo.toml\n'
[ "$touch_lock" = 0 ] || printf 'changed: Cargo.lock\n'
[ "$touch_page" = 0 ] || printf 'changed: Dockerfile\n'
