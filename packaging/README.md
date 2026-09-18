# Packaging and deployment

Two ways to run `selvaged` beyond `cargo run`, for two audiences:

| Path | Who it is for | What it is |
|---|---|---|
| `systemd/` | The Pi demo today, a VPS demo later | A user unit plus install docs — the proven path, generalized |
| `Dockerfile` + `compose.yaml` | Strangers self-hosting on their own machines | A multi-arch image and a one-service compose file — never the Pi |

The server is memory-only under both: restarts end all rooms, and there is
nothing to persist — hence no data volume anywhere here. The one mount is the
page directory, read-only, which the server only ever reads. `DESIGN.md` §9
names the missing deployment story; this directory is that story's first half
(the second half, a live VPS, is still unordered spend).

## FSL-1.1-MIT redistribution review — required before any future publish

`crates/selvaged` is source-available under `FSL-1.1-MIT`, not open source.
Handing the binary to someone else — pushing the image to GHCR, or any
third-party re-host — is **redistribution of the binary**, permitted for
Permitted Purposes and forbidden for **Competing Use** (a commercial
product or service that substitutes for the software). That review has **not**
happened: what GHCR distribution and third-party re-hosts count as Competing
Use, the exact package visibility, and the wording below are open items for
the owner, not decisions this directory takes.

What is already in place for that review:

- The licence text ships **inside the image** (`/LICENSE`, copied from
  `crates/selvaged/LICENSE`) and in its annotations
  (`org.opencontainers.image.licenses=FSL-1.1-MIT`, alongside title, source,
  version and revision labels).
- The binary stays in its own layer; no MIT/Apache crate's binary is copied
  into the same tag in a way that confuses the licence story. (The harness
  links `selvaged`, so a redistributed harness binary carries FSL code — the
  top-level README already says so, and the image never ships one.)
- No image has been pushed anywhere. The publish leg of
  `.github/workflows/image.yml` runs on release tags only, no tag has been
  cut, and none will be cut from this work.

## systemd user unit

`systemd/selvaged.service` is the Pi demo's hand-written unit, generalized:
absolute binary path, explicit `--listen`, explicit `--room-grace-ms`,
`Restart=always`, linger, and file-append logging. `%h` expands to the
installing user's home, so the file installs as-is.

### Install

Build the binary with provenance, then install (user scope — no root, no
packages):

```sh
cargo build --release --locked -p selvaged   # note the source SHA
packaging/systemd/install.sh ./target/release/selvaged
```

The script copies the binary to `~/selvage/selvaged`, installs the unit,
reloads the daemon, enables linger, and starts the service. Without the
script, the same steps by hand:

```sh
install -D -m755 ./target/release/selvaged ~/selvage/selvaged
install -D -m644 packaging/systemd/selvaged.service ~/.config/systemd/user/selvaged.service
cp packaging/systemd/selvaged.env.example ~/selvage/selvaged.env  # then edit the bind address
systemctl --user daemon-reload
loginctl enable-linger "$USER"
systemctl --user enable --now selvaged
```

The bind address is yours, not the demo's: name an address your guests can
reach (a tailnet IP, a VPS address) — never loopback when someone else is
joining. `0.0.0.0:8080` listens everywhere; then the guests' URL must name
the machine, not the wildcard. Anything absent from `selvaged.env` falls back
to the unit's defaults (loopback `:8080`, 30 s grace), so a missing file is a
local-only server.

### Operate

```sh
systemctl --user is-active selvaged
curl -sS http://<your-address>:8080/meta   # answers selvaged/<version> with selvage/1
tail -f ~/selvage/selvaged.log             # the log — not the journal
systemctl --user restart selvaged          # rooms die; the process returns in seconds
```

Logs: the unit appends stdout to `~/selvage/selvaged.log`, and that file is
the record. The demo Pi's journal keeps no user-unit output at all, and
enabling journal persistence for user units needs root — which this path
never takes. Expect the startup lines (`selvaged listening on …`, the grace
note) re-logged after every restart.

Memory-only semantics: `restart`, a kill, or a reboot ends **all** rooms
instantly. Survival means the process returns, not the rooms. A host that
disconnects while guests remain gets the grace window: rejoin with the same
invite inside it to reclaim the room, mint a fresh one after it.

### Upgrade

Replace the binary, restart, check, record:

```sh
cargo build --release --locked -p selvaged   # at the new source SHA
install -m755 ./target/release/selvaged ~/selvage/selvaged
systemctl --user restart selvaged
selvaged --version 2>/dev/null; ~/selvage/selvaged --version
curl -sS http://<your-address>:8080/meta
```

After every upgrade the log must show the startup lines and `--version` and
`/meta` must agree; record the new source SHA the way the Pi runbook does.

### Provenance notes

- `systemd-analyze verify` passes on the shipped unit after specifier
  substitution (verified with `%h` pointed at a scratch home containing the
  expected paths). Unsubstituted, its only finding is that the binary is not
  installed yet — the expected state on a machine without an install.
- Reboot recovery is proven on the demo Pi (linger on, unit enabled) but that
  proof belongs to the Pi's own session, not to this file: a fresh machine
  should close it the same way (`sudo reboot`, then `/meta` and `is-active`
  with no session held).

## Container image

Multi-arch (`linux/amd64` + `linux/arm64`): the arm64 variant is for other
people's Pis and ARM VPSes, not for this Pi, which already runs a native
binary. Build locally:

```sh
docker buildx build -t selvaged:local .
docker run --rm -p 8080:8080 selvaged:local
curl -sS http://127.0.0.1:8080/meta
```

Primary shape is a musl-static binary on `scratch`: the lockfile carries no
TLS C shims (no openssl/ring/aws-lc-sys — verified by grep over `Cargo.lock`;
only `libc`/`mio` as OS shims), so the static build needs no C libraries, and
the runtime holds one binary plus `/LICENSE` as non-root user 65532, no
shell, no package manager. The `Dockerfile` cross-compiles both
architectures natively on an x86_64 builder (one cross-toolchain stage per
target, each pinned `--platform=$BUILDPLATFORM`), so building arm64 needs
no QEMU — only *running* the arm64 image does. Build and run it on any
machine with a working Docker; the CI runners are not such machines (see
CI below), so the Dockerfile is human-verified, not CI-proven.

Fallback is the same static binary on `gcr.io/distroless/static:nonroot`
(`--build-arg RUNTIME=distroless`). If a future dependency ever breaks the
musl build, compile a glibc binary instead and keep the distroless runtime;
the stage already accepts any binary at `/selvaged`.

Inside the container `--listen 0.0.0.0:8080` is correct *because* the port
mapping supplies the boundary — contrast the unit above, which binds the
reachable address explicitly.

### Tags and version truthfulness

Image tags are `<cargo-version>-<short-sha>` (e.g. `0.1.0-e617d81`), computed
by `scripts/image-tag.sh`, plus moving `<version>`/`latest` aliases only on
published releases. `--version`, `/meta`, and `CARGO_PKG_VERSION` are already
wired together in code (`Meta::reference`, test-enforced by
`version_matches_what_meta_serves`); `scripts/check-server-version.sh`
asserts the same truthfulness against a running server, and
`scripts/image-smoke.sh` runs the whole smoke without publishing and
without Docker: it builds `packages.image` (the nix/dockerTools image
below), verifies manifest and container config with skopeo, extracts the
exact binary from the image layers, and checks `--version` and `/meta`
against it. `scripts/ci-local.sh image` runs that smoke anywhere nix does.

### Compose

`compose.yaml` is one service, one port mapping, `restart: unless-stopped`,
an overridable `command`, and one read-only page mount — the server keeps
nothing on disk of its own. `docker compose up`, then `curl /meta`, then host a
room from an editor. Until the first image is published the file builds locally
(`build:`); after that it pulls the published tag.

### One origin: the page

`selvaged --serve-page DIR` serves the browser page on the same origin as
`/session` and `/meta` (see the root README). The container recipe mounts the
page directory read-only at `/page` and passes the flag, so one container and
one port answer the page, the meta document and the socket — the CORS proxy and
the second page server the Pi runbook hand-writes are not needed.

**The page is not in this repository.** It is the `web_client` repository's
built `dist/`; this repository neither builds nor vendors it, so the image
ships the server alone and the page is mounted (`-v …/dist:/page:ro`). A build
that pulls it in — a Dockerfile stage that clones `web_client` at a pinned
revision — is the obvious next step and is **not** done: this environment has
no reachable copy of that repository to pin or to verify against. `./page`
absent is not fatal: `/meta` and `/session` still answer and `/` is a `404`.

### TLS

`selvaged` speaks no TLS and claims none: an invite over `ws://` is plaintext,
and an `https` page cannot dial a `ws://` socket (mixed active content). For a
shareable link, put a terminator in front — `tailscale serve`, caddy, or your
edge — and hand out the `https://`/`wss://` URL. Serving the page from the same
origin as the socket is what makes that one terminator enough.

## CI

`.github/workflows/image.yml` has three jobs. `smoke` runs on every PR and on
`main`, one leg per architecture (`amd64` on the usual Blacksmith runners,
`arm64` on GitHub-hosted ARM — each building natively, no cross-compilation): the
same nix preamble as the checks job, then `scripts/image-smoke.sh`, which
needs no Docker daemon at all. That is deliberate, not incidental: nine CI
rounds established that these runners cannot execute buildx builds (daemon,
setup actions, pulls and builder bootstrap all green; every build dead in
seconds, including `FROM scratch`), so the smoke builds the image the way
the runners provably can — `nix build .#image` — and verifies it with
skopeo. The `Dockerfile` above stays the portable static variant for
machines with a working Docker; both agree on entrypoint, port, user,
licence label and tag scheme, and the smoke asserts exactly those fields.

`container` is the other half, and the only job that runs the image:
GitHub-hosted `ubuntu-24.04` (chosen for its Docker daemon and unrestricted
egress — the Blacksmith runners proved only github-scoped egress and could not
execute buildx builds), a plain `docker build` of the `Dockerfile`, `docker
run` with a page mounted, and `scripts/container-smoke.sh`: it asserts the
container's `--version` and `/meta` against `Cargo.toml`, asserts the page's
headers and bytes over the container's port, and then mints a room in the
container and joins it as a guest with the harness's client engine
(`crates/harness/examples/join_room.rs`), converging on an edit. A `--version`
or `/meta` check is not that proof: no client had ever completed a handshake
against the image. `scripts/ci-local.sh container` runs the same script where a
Docker daemon exists.

`publish` needs `smoke`, carries the only
elevated permission in the repo (`packages: write`), logs in to GHCR with the
built-in `GITHUB_TOKEN`, and is gated on release tags (`refs/tags/v*`) — tags
that are never cut from this work. It builds with Docker while the smoke
builds with nix, so it must itself be re-proven green on a real run before
any first publish. FSL review stays open until the owner
closes it; the code merges, the artifacts do not.

### The page in a container

The page is not baked into the image (above), so a container that serves one
mounts it; `scripts/container-smoke.sh` does exactly that with a three-file
page and asserts the header policy a browser depends on: the pinned media type,
`no-cache` for a stable name and `public, max-age=31536000, immutable` for a
content-hashed one, `Referrer-Policy: no-referrer`, `X-Content-Type-Options:
nosniff`, and the content security policy. Those headers are the static
handler's own (`crates/selvaged/src/page.rs`) and pinned in
`crates/harness/tests/page.rs` as well, so the container job proves them where
they are actually served rather than only in-process.
