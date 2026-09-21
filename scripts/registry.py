"""The anonymous registry reads that `scripts/`' image tools share.

Both images are public on GHCR, so they are read the way `docker pull` reads
them and with no credential: a bare manifest request, the `WWW-Authenticate`
challenge that comes back, a pull-scoped bearer token minted from
`ghcr.io/token`, then the same request again with that token. One copy of that
dance, because it is the part that is easy to get subtly wrong and there is
nothing in it specific to what a caller wants to do with the answer.

`assert-multiarch-layers.py` uses `fetch_index`/`fetch_manifest`/`fetch_blob` to
compare what each platform's manifest actually carries; `image-digest.py` uses
`index_digest` to turn a release tag into the reference the deployment pins.
"""

import json
import urllib.error
import urllib.request

INDEX_ACCEPT = (
    "application/vnd.oci.image.index.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json"
)
MANIFEST_ACCEPT = (
    "application/vnd.oci.image.manifest.v1+json,"
    "application/vnd.docker.distribution.manifest.v2+json"
)


def parse_ref(ref):
    if "@" in ref:
        raise SystemExit(f"pin by tag, not digest: {ref!r}")
    host_and_repo, _, tag = ref.rpartition(":")
    if not host_and_repo or not tag:
        raise SystemExit(f"expected <host>/<repo>:<tag>, got {ref!r}")
    host, _, repo = host_and_repo.partition("/")
    if not repo:
        raise SystemExit(f"expected <host>/<repo>:<tag>, got {ref!r}")
    return host, repo, tag


def fetch_token(challenge):
    # The challenge is `Bearer realm="<endpoint>",service="…",scope="…"`: every field
    # other than the realm is a query parameter on it, and the scope is what makes
    # the token read-only.
    if not challenge.startswith("Bearer "):
        raise SystemExit(f"unsupported auth challenge: {challenge!r}")
    params = {}
    for part in challenge[len("Bearer ") :].split(","):
        k, _, v = part.partition("=")
        params[k.strip()] = v.strip().strip('"')
    realm = params.pop("realm")
    query = "&".join(f"{k}={v}" for k, v in params.items())
    with urllib.request.urlopen(f"{realm}?{query}", timeout=30) as resp:
        return json.loads(resp.read())["token"]


def registry_get(host, path, accept, token=None):
    """`(body, headers)` for one registry path, minting a token if it is challenged."""
    scheme = "http" if host.startswith(("localhost", "127.0.0.1")) else "https"
    url = f"{scheme}://{host}{path}"
    headers = {"Accept": accept}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read(), resp.headers
    except urllib.error.HTTPError as e:
        if e.code == 401 and token is None:
            new_token = fetch_token(e.headers.get("WWW-Authenticate", ""))
            return registry_get(host, path, accept, token=new_token)
        raise SystemExit(f"GET {url} failed: {e.code} {e.reason}") from e


def index_digest(host, repo, tag):
    """The digest of the image index a tag names, as the registry reports it.

    The digest is the registry's own answer rather than a hash this recomputes:
    it is what a pull resolves the tag against, and it is what makes the
    deployment's pin immune to the tag being repointed later.
    """
    _, headers = registry_get(host, f"/v2/{repo}/manifests/{tag}", INDEX_ACCEPT)
    digest = headers.get("Docker-Content-Digest")
    if not digest:
        raise SystemExit(f"{host}/{repo}:{tag} answered with no Docker-Content-Digest")
    return digest
