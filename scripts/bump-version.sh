#!/usr/bin/env bash
#
# The version a release is cut at, one bump word at a time, from the version this
# repository already carries.
#
#   scripts/bump-version.sh <major|minor|patch> [--dry-run]
#
# The version it moved to is the last line of stdout, and the only line on
# stdout: the release workflow names the tag, the image and the Release from that
# line, so nothing else may share it. Everything it says about what it did goes to
# stderr.
#
# What it writes:
#
#   Cargo.toml   `[workspace.package] version`, the string a release tag is
#                asserted against (`scripts/release-tags.sh`). Which string each
#                artefact keeps is the release runbook's table.
#   Cargo.lock   regenerated with cargo (`update --workspace`), never edited by
#                hand: the four local packages carry the workspace version, and a
#                lock that disagrees with the manifest fails the build.
#
# Leave anything that is a *home* for a different number (a dispatch input's
# example, a fixture) to its own file: the two above are the only files this
# moves, and each only because a release writes it.
#
# `--dry-run` prints the version it would move to and writes nothing, so a caller
# can compute the tag before it has decided to cut it.
#
# Nothing is written for a refusal. Exit 2 refuses the arguments — a word that is
# not one of the three, a second word, an unknown option, and a second
# `--dry-run` — and exit 1 refuses the tree:
# a manifest that does not carry a plain `X.Y.Z` (fewer than three parts, a
# prerelease, a leading zero) has no next version to compute, and a file that is
# not there has none either. A failure after an edit restores what it had touched,
# so a failed run is not a half-bumped tree for someone to commit.
set -euo pipefail

usage='usage: scripts/bump-version.sh <major|minor|patch> [--dry-run]'

refuse() {
    printf 'refused: %s\n%s\n' "$*" "$usage" >&2
    exit 2
}

fail() {
    printf 'failed: %s\n' "$*" >&2
    exit 1
}

bump=''
dry_run=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)
            [ "$dry_run" = 0 ] || refuse '--dry-run is given twice'
            dry_run=1
            shift
            ;;
        -*)
            refuse "unknown option $1"
            ;;
        *)
            [ -z "$bump" ] || refuse "more than one bump word given: $bump and $1"
            bump=$1
            shift
            ;;
    esac
done

case "$bump" in
    major | minor | patch) ;;
    '') refuse 'no bump word given: say major, minor or patch' ;;
    *) refuse "not a bump word: $bump (major, minor or patch)" ;;
esac

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

for file in Cargo.toml Cargo.lock; do
    [ -f "$file" ] || fail "$repo_root/$file is not there"
done

read_manifest_version() {
    local found
    found=$(grep -c '^version = "' Cargo.toml || true)
    [ "$found" = 1 ] ||
        fail "Cargo.toml has $found top-level version lines; want exactly one, its [workspace.package] version"
    sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml
}

# The next version, or non-zero when `current` is not a plain `MAJOR.MINOR.PATCH`
# to compute from: three decimal parts, no leading zero. A prerelease or a
# two-part version has no unambiguous next version, so the caller is told rather
# than handed a guess.
next_version() {
    local current="$1" major minor patch
    [[ $current =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || return 1
    major=${BASH_REMATCH[1]}
    minor=${BASH_REMATCH[2]}
    patch=${BASH_REMATCH[3]}
    case "$bump" in
        major) printf '%s.0.0\n' "$((major + 1))" ;;
        minor) printf '%s.%s.0\n' "$major" "$((minor + 1))" ;;
        patch) printf '%s.%s.%s\n' "$major" "$minor" "$((patch + 1))" ;;
    esac
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
        [ "$member_version" = "$1" ] ||
            { printf '%s is %s, not %s\n' "$member" "$member_version" "$1" >&2; return 1; }
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

current=$(read_manifest_version)
if ! version=$(next_version "$current"); then
    fail "Cargo.toml carries $current, which is not a plain MAJOR.MINOR.PATCH; there is no next version to compute"
fi

printf 'version %s -> %s\n' "$current" "$version" >&2

if [ "$dry_run" = 1 ]; then
    printf '%s\n' "$version"
    exit 0
fi

# The originals, so a cargo that refuses ends with the tree it began with.
# Inside the checkout: nowhere here writes to a system temporary directory.
work="$repo_root/.tmp/bump-version.$$"
mkdir -p "$work"
restore() {
    cp -p "$work/Cargo.toml" Cargo.toml
    cp -p "$work/Cargo.lock" Cargo.lock
}
# The `.tmp` directory goes too when it was made for this run and nothing else is
# in it, so a bumped tree is the tree it was plus the files.
trap 'rm -rf "$work"; rmdir "$repo_root/.tmp" 2>/dev/null || true' EXIT
cp -p Cargo.toml Cargo.lock "$work/"

set_manifest_version "$version" ||
    { restore; fail 'Cargo.toml has no [workspace.package] version line to set'; }
if ! run_cargo update --workspace; then
    restore
    fail 'cargo update --workspace failed; Cargo.toml and Cargo.lock are as they were'
fi

if [ "$(read_manifest_version)" != "$version" ]; then
    restore
    fail "Cargo.toml still does not carry $version; the tree is as it was"
fi
if ! lock_is_current "$version"; then
    restore
    fail "Cargo.lock does not carry $version for every local package; the tree is as it was"
fi

printf 'changed: Cargo.toml\n' >&2
printf 'changed: Cargo.lock\n' >&2
printf '%s\n' "$version"
