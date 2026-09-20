#!/usr/bin/env python3
"""Assert a published image's amd64 and arm64 legs are genuinely different builds.

    scripts/assert-multiarch-layers.py <registry-host>/<repo>:<tag>

Reads the OCI image index directly from the registry's HTTP API (the same
anonymous bearer-token dance `docker pull` does, or no auth at all for an
insecure loopback registry), then two things a green `docker image inspect`
does not prove:

1. The `linux/amd64` and `linux/arm64` platform manifests must not carry the
   same layer digests. A cross-compiled binary in one and a copy of the other
   platform's binary republished under a different config produce identical
   layer digests even though the config blob says otherwise; this catches it
   without running anything, from the manifests alone.
2. Each platform's `/selvaged` binary, pulled straight out of its layer, must
   have the ELF `e_machine` its platform claims (`0x3E`/62 for amd64,
   `0xB7`/183 for aarch64) - `docker image inspect`'s `.Architecture` is
   metadata BuildKit writes from the requested `--platform`, independent of
   what the layer actually contains, so only the bytes themselves prove it.

Stdlib only, host is the registry the CI runners can reach with no
credentials for this public package. Exit non-zero, naming what disagreed.
"""

import io
import json
import sys
import tarfile
import urllib.error
import urllib.request

ELF_MACHINE = {"amd64": 62, "arm64": 183}
WANTED_PLATFORMS = ("amd64", "arm64")


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


def registry_get(host, path, accept, token=None):
    scheme = "http" if host.startswith(("localhost", "127.0.0.1")) else "https"
    url = f"{scheme}://{host}{path}"
    headers = {"Accept": accept}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read(), resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        if e.code == 401 and token is None:
            challenge = e.headers.get("WWW-Authenticate", "")
            new_token = fetch_token(challenge)
            return registry_get(host, path, accept, token=new_token)
        raise SystemExit(f"GET {url} failed: {e.code} {e.reason}") from e


def fetch_token(challenge):
    # Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:x:pull"
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


INDEX_ACCEPT = (
    "application/vnd.oci.image.index.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json"
)
MANIFEST_ACCEPT = (
    "application/vnd.oci.image.manifest.v1+json,"
    "application/vnd.docker.distribution.manifest.v2+json"
)


def fetch_index(host, repo, tag):
    body, _ = registry_get(host, f"/v2/{repo}/manifests/{tag}", INDEX_ACCEPT)
    index = json.loads(body)
    platforms = {}
    for m in index.get("manifests", []):
        arch = m.get("platform", {}).get("architecture")
        if arch in WANTED_PLATFORMS and arch not in platforms:
            platforms[arch] = m["digest"]
    missing = [a for a in WANTED_PLATFORMS if a not in platforms]
    if missing:
        raise SystemExit(
            f"index for {host}/{repo}:{tag} is missing platform(s) {missing}; "
            f"found {sorted(platforms)}"
        )
    return platforms


def fetch_manifest(host, repo, digest):
    body, _ = registry_get(host, f"/v2/{repo}/manifests/{digest}", MANIFEST_ACCEPT)
    return json.loads(body)


def fetch_blob(host, repo, digest):
    body, _ = registry_get(
        host, f"/v2/{repo}/blobs/{digest}", "application/octet-stream"
    )
    return body


def extract_binary(host, repo, layer_digests):
    for digest in layer_digests:
        blob = fetch_blob(host, repo, digest)
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.lstrip("./") == "selvaged":
                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    return f.read(20)
    return None


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <host>/<repo>:<tag>", file=sys.stderr)
        return 2
    host, repo, tag = parse_ref(sys.argv[1])

    platforms = fetch_index(host, repo, tag)
    manifests = {
        arch: fetch_manifest(host, repo, digest)
        for arch, digest in platforms.items()
    }
    layers = {
        arch: [layer["digest"] for layer in manifests[arch]["layers"]]
        for arch in WANTED_PLATFORMS
    }
    print(f"amd64 layers: {layers['amd64']}")
    print(f"arm64 layers: {layers['arm64']}")

    failures = []
    if layers["amd64"] == layers["arm64"]:
        failures.append(
            "linux/amd64 and linux/arm64 carry the exact same layer digests: "
            "the arm64 leg was not built separately from the amd64 one"
        )
    else:
        print("layer digests OK: amd64 and arm64 differ")

    for arch in WANTED_PLATFORMS:
        header = extract_binary(host, repo, layers[arch])
        if header is None:
            failures.append(f"linux/{arch}: no file named 'selvaged' in any layer")
            continue
        if header[:4] != b"\x7fELF":
            failures.append(f"linux/{arch}: /selvaged is not an ELF binary")
            continue
        machine = header[18]
        want = ELF_MACHINE[arch]
        if machine != want:
            failures.append(
                f"linux/{arch}: /selvaged has ELF e_machine {machine}, "
                f"want {want}"
            )
        else:
            print(f"ELF OK on linux/{arch}: e_machine {machine}")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"multiarch OK: {host}/{repo}:{tag} carries a genuine binary per platform")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
