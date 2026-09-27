#!/usr/bin/env bash
#
# The `web_client` revision the server image bakes into the page it serves.
#
#   scripts/page-revision.sh
#
# Prints, on the only line of stdout, the commit of `selvage-protocol/web_client`'s
# latest published release: the tag `gh api repos/<repository>/releases/latest`
# names, peeled to the commit it points at. Everything else goes to stderr.
#
# Who reads it: the release workflow, which hands the value to
# `scripts/bump-version.sh --page-sha`. That is what moves `Dockerfile`'s
# `ARG WEB_CLIENT_SHA=`, so the pin stays committed — the tree names the revision
# the image bakes, and a build from it is reproducible — while the page that
# revision comes from is the one this release is cut beside rather than one from
# two releases ago.
#
# A tag is one of two objects and this has to read both. An **annotated** tag is an
# object of its own: `git ls-remote` prints it as `refs/tags/<tag>` and, only when
# that ref is asked for by name *and* the peeled one is too, a second
# `refs/tags/<tag>^{}` line naming the commit. A **lightweight** tag *is* the
# commit and has no peeled line. Asking for the plain pattern alone therefore
# answers an annotated tag with the tag object, which is not a commit a
# `git fetch --depth 1 origin <sha>` can take, so both patterns are asked for and
# the peeled line wins when it is there.
#
# A refusal (exit 1, nothing on stdout) is a release that cannot be cut with its
# page pinned to anything real: no latest release to read, a tag that resolves to
# nothing on the remote, or a value that is not a 40-hex commit.
set -euo pipefail

repository=${SELVAGE_WEB_CLIENT_REPOSITORY:-selvage-protocol/web_client}
remote=${SELVAGE_WEB_CLIENT_REMOTE:-https://github.com/selvage-protocol/web_client}

refuse() {
    printf 'refused: %s\n' "$*" >&2
    exit 1
}

if ! tag=$(gh api "repos/$repository/releases/latest" --jq .tag_name 2>/dev/null); then
    refuse "cannot read $repository's latest release (repos/$repository/releases/latest failed; a repository with no release answers 404)"
fi
[ -n "$tag" ] || refuse "$repository's latest release names no tag"

refs=$(git ls-remote "$remote" "refs/tags/$tag" "refs/tags/$tag^{}" 2>/dev/null) ||
    refuse "cannot read $remote's tags"

sha=$(printf '%s\n' "$refs" | awk -v want="refs/tags/$tag^{}" '$2 == want { print $1 }')
[ -n "$sha" ] ||
    sha=$(printf '%s\n' "$refs" | awk -v want="refs/tags/$tag" '$2 == want { print $1 }')
[ -n "$sha" ] || refuse "$remote has no tag $tag; the release it names is not on the remote"

[[ $sha =~ ^[0-9a-f]{40}$ ]] || refuse "$tag resolves to $sha, which is not a 40-hex commit"
printf '%s\n' "$sha"
