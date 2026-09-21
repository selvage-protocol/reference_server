#!/usr/bin/env python3
"""Turn a release version into the digest-pinned reference the deployment records.

    scripts/image-digest.py ghcr.io/selvage-protocol/selvaged:0.2.0
    sha256:98022660aeb788d27658c54ba0bfce736bc36785b79d0bdc1a4ee15b8a41cc3c

The deploy workflow resolves each version this way before it touches the box, and
writes `ghcr.io/selvage-protocol/<repo>@<digest>` into `/etc/selvage/.env`. A
digest rather than the tag, because a tag is a name the publisher can repoint:
the deployment's record of what runs has to survive that, and a rollback has to
be reproducible.

**A version, not a tag.** Only `MAJOR.MINOR.PATCH` is accepted, so neither
`latest` nor a commit-stamped tag like `0.1.0-37bb674` can be deployed by
dispatch: what a public origin runs is a release someone named, not whatever the
registry happens to point at today. That is also why the pattern lives here
rather than in the workflow — this is the thing that decides.

One real registry read per call, anonymous, using the same token flow
`assert-multiarch-layers.py` does (`registry.py`). Nothing is cached: a digest
resolved from a stale cache is exactly the failure the pin exists to prevent.
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import registry  # noqa: E402  (the path above is what makes this importable)

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


def pinned_reference(ref):
    """`<host>/<repo>:<version>` to `(<host>/<repo>, <digest>)`, or a refusal."""
    host, repo, version = registry.parse_ref(ref)
    if not VERSION_PATTERN.match(version):
        raise SystemExit(
            f"{version!r} is not a MAJOR.MINOR.PATCH release version: "
            f"a moving tag cannot be deployed"
        )
    return f"{host}/{repo}", registry.index_digest(host, repo, version)


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <host>/<repo>:<version>", file=sys.stderr)
        return 2
    _, digest = pinned_reference(sys.argv[1])
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
