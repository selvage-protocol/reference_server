#!/usr/bin/env bash
#
# The version a release is cut at, one bump word at a time, from the version this
# repository already carries.
#
#   scripts/bump-version.sh <major|minor|patch> [--page-sha <40 lowercase hex>] [--dry-run]
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
#                artefact keeps is `docs/runbook-release.md` §1's table.
#   Cargo.lock   regenerated with cargo (`update --workspace`), never edited by
#                hand: the four local packages carry the workspace version, and a
#                lock that disagrees with the manifest fails the build.
#   Dockerfile   `ARG WEB_CLIENT_SHA=`, and **only with `--page-sha`**: the
#                `web_client` revision the page stage bakes beside the binary,
#                which `scripts/page-revision.sh` names from that repository's
#                latest release. A pin a release moves by hand is a pin that rots,
#                and this one did — it stood two releases behind — so the release
#                workflow resolves the revision and hands it here rather than
#                editing the file itself. The pin stays *committed*, so a build
#                from the tree is reproducible, and a release moves it, so it
#                stops lagging. Given the revision the file already carries, this
#                writes nothing and says so.
#
# Leave anything that is a *home* for a different number (a dispatch input's
# example, a fixture) to its own file: the three above are the only files this
# moves, and each only because a release writes it.
#
# `--dry-run` prints the version it would move to and writes nothing, so a caller
# can compute the tag before it has decided to cut it.
#
# Nothing is written for a refusal. Exit 2 refuses the arguments — a word that is
# not one of the three (including the `X.Y.Z` form this used to take), a second
# word, an unknown option, a second `--dry-run`, and a `--page-sha` that is not 40
# lowercase hex — and exit 1 refuses the tree: a manifest that does not carry a
# plain `X.Y.Z` (fewer than three parts, a prerelease, a leading zero) has no next
# version to compute, a file that is not there has none either, and a `Dockerfile`
# without exactly one `ARG WEB_CLIENT_SHA=` line has nowhere to put a revision. A
# failure after an edit restores what it had touched, so a failed run is not a
# half-bumped tree for someone to commit.
set -euo pipefail

usage='usage: scripts/bump-version.sh <major|minor|patch> [--page-sha <40 lowercase hex>] [--dry-run]'

refuse() {
    printf 'refused: %s\n%s\n' "$*" "$usage" >&2
    exit 2
}

fail() {
    printf 'failed: %s\n' "$*" >&2
    exit 1
}

bump=''
page_sha=''
page_sha_given=0
dry_run=0
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)
            [ "$dry_run" = 0 ] || refuse '--dry-run is given twice'
            dry_run=1
            shift
            ;;
        --page-sha)
            [ $# -ge 2 ] || refuse '--page-sha needs a value'
            [ "$page_sha_given" = 0 ] || refuse '--page-sha is given twice'
            page_sha=$2
            page_sha_given=1
            shift 2
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

# The one grammar the page stage's `git fetch --depth 1 origin <sha>` can take. An
# empty value is not "no value": it is a caller that meant to name a revision and
# has none, which is the hand-maintained-pin failure this option exists to close.
if [ "$page_sha_given" = 1 ] && ! [[ $page_sha =~ ^[0-9a-f]{40}$ ]]; then
    refuse "not 40 lowercase hex characters: $page_sha"
fi

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

for file in Cargo.toml Cargo.lock; do
    [ -f "$file" ] || fail "$repo_root/$file is not there"
done
if [ "$page_sha_given" = 1 ]; then
    [ -f Dockerfile ] || fail "$repo_root/Dockerfile is not there"
    found=$(grep -c '^ARG WEB_CLIENT_SHA=' Dockerfile || true)
    [ "$found" = 1 ] ||
        fail "Dockerfile has $found ARG WEB_CLIENT_SHA= lines; want exactly one, the revision the page stage bakes"
fi

read_manifest_version() {
    local found
    found=$(grep -c '^version = "' Cargo.toml || true)
    [ "$found" = 1 ] ||
        fail "Cargo.toml has $found top-level version lines; want exactly one, its [workspace.package] version"
    sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml
}

read_page_sha() {
    sed -n 's/^ARG WEB_CLIENT_SHA=\(.*\)$/\1/p' Dockerfile
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

set_page_sha() {
    local temporary="$work/Dockerfile.new"
    cp -p Dockerfile "$temporary"
    awk -v new="$1" '
        /^ARG WEB_CLIENT_SHA=/ {
            print "ARG WEB_CLIENT_SHA=" new; found = 1; next
        }
        { print }
        END { if (!found) exit 3 }
    ' Dockerfile >"$temporary" || return 1
    mv "$temporary" Dockerfile
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

# The originals, so a cargo that refuses or a file that has lost the line it was
# to carry ends with the tree it began with. Inside the checkout: nowhere here
# writes to a system temporary directory.
work="$repo_root/.tmp/bump-version.$$"
mkdir -p "$work"
page_sha_report=''
restore() {
    cp -p "$work/Cargo.toml" Cargo.toml
    cp -p "$work/Cargo.lock" Cargo.lock
    [ "$page_sha_given" = 0 ] || cp -p "$work/Dockerfile" Dockerfile
}
# The `.tmp` directory goes too when it was made for this run and nothing else is
# in it, so a bumped tree is the tree it was plus the files.
trap 'rm -rf "$work"; rmdir "$repo_root/.tmp" 2>/dev/null || true' EXIT
cp -p Cargo.toml Cargo.lock "$work/"
[ "$page_sha_given" = 0 ] || cp -p Dockerfile "$work/"

# The page revision first: it is the one write that may have nothing to do, and a
# tree already carrying it is left byte for byte as it was.
if [ "$page_sha_given" = 1 ]; then
    if [ "$(read_page_sha)" = "$page_sha" ]; then
        page_sha_report='unchanged: Dockerfile (it already carries the revision)'
    else
        set_page_sha "$page_sha" ||
            { restore; fail 'Dockerfile has no ARG WEB_CLIENT_SHA= line to set'; }
        page_sha_report='changed: Dockerfile'
    fi
fi

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
[ -z "$page_sha_report" ] || printf '%s\n' "$page_sha_report" >&2
printf '%s\n' "$version"
