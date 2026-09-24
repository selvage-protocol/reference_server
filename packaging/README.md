# Packaging and deployment

Five shapes, for five audiences:

| Path | Who it is for | What it is |
|---|---|---|
| `pi/` | The live Pi demo, exactly as it runs | The two-service compose file for the tailnet, the environment file that names both images by digest, and the deploy script that box's CI user may run as root |
| `pi-demo/` | The Pi's native shape, retired 2026-09-21 | The TLS front and the user units it ran — tracked here so a deployment artefact is reviewable, and left on the machine as the rollback recipe |
| `prod/` | The live public demo, exactly as it runs | The TLS front, the router and the three-service compose file for one public origin — the Pi's container pattern plus a front that terminates TLS and routes — plus the deploy script that box's CI user may run as root |
| `systemd/` | A self-hoster on their own machine | A user unit plus install docs — the proven path, generalized, with no front |
| `Dockerfile` + `compose.yaml` | Strangers self-hosting on their own machines | A multi-arch image and a one-service compose file — never the Pi |
The server is memory-only under all five: restarts end all rooms, and
there is nothing to persist — hence no data volume anywhere here. The image
carries its page; the mounts are the optional read-only page override, which the
server only ever reads, and the public demo's origin certificate, which is what
its front terminates TLS with (`prod/README.md`).
`DESIGN.md` §9 names the missing deployment story; this directory is that
story's first half.

## FSL-1.1-MIT redistribution — accepted by the owner, 2026-09-19

`crates/selvaged` is source-available under `FSL-1.1-MIT`, not open source.
Handing the binary to someone else — pushing the image to GHCR, or any
third-party re-host — is **redistribution of the binary**: the licence permits
it for any Permitted Purpose and forbids it for a **Competing Use** (making the
software available to others in a commercial product or service that
substitutes for the software, for something else we offer, or one that offers
the same or substantially similar functionality), and its Redistribution
clause carries those terms onto every copy.

**The owner accepted that scope for this project's publication on 2026-09-19.**
Publishing the built image to GHCR as `ghcr.io/selvage-protocol/selvaged`, where
anyone may pull it, and re-hosting that published image, are within the
licence's Permitted Purpose as the owner reads it, and are not a Competing Use.
Read that as the owner's own acceptance of these terms for these artefacts —
not as a legal opinion, and not as permission for anything this project does
not publish. The licence also grants a future one: each release's terms become
MIT on the second anniversary of the release.

What the acceptance stands on, all of it in the artefacts themselves:

- The licence text ships **inside the image** (`/LICENSE`, copied from
  `crates/selvaged/LICENSE`) and in its annotations
  (`org.opencontainers.image.licenses=FSL-1.1-MIT`, alongside title, source,
  version, revision and the page's revision labels).
- The binary stays in its own layer; no MIT/Apache crate's binary is copied
  into the same tag in a way that confuses the licence story. (The harness
  links `selvaged`, so a redistributed harness binary carries FSL code — the
  top-level README already says so, and the image never ships one.)
- A published tag names the revision it was built from (`<version>-<sha>`); the
  moving `<version>` and `latest` aliases follow only a release.
- Nothing here says a *service* built on the software is permitted: a
  commercial product or service that substitutes for it is the Competing Use
  the licence still forbids.

## The Pi demo's shapes (`pi/`, `pi-demo/`)

`pi/` is what the Pi runs now, tracked file for file: the two-service compose
file for the tailnet (the page on 80, the server on 8080), the environment file
that names both images by digest, the deploy script `deployci` may run as root,
and the pin that bounds the host's image updater. `pi-demo/` beside it is the
native shape this replaced — the `selvaged` user unit, the stdlib TLS front in
front of it, the front's unit and the front's environment documented — stopped on
2026-09-21 and left in place as the rollback recipe. Both are deployment records
as much as recipes: each `README.md` says what every file installs to and how to
roll back, and `ai_notes/docs/runbook-pi-demo.md` owns the live state.

**The native shape is not the container path.** The container's port mapping is
its boundary, so it binds `0.0.0.0:8080` and needs no front; the Pi's boundary is
the tailnet, so the native shape bound the tailnet address with a TLS terminator
in front of it because `selvaged` speaks no TLS. What runs there now needs no
terminator: the two images publish the tailnet address themselves, and one of
them is the page, so nothing routes. Nor is `pi-demo/` the `systemd/` unit above,
which is the same binary with no front — a self-hoster supplies TLS with
`tailscale serve`, caddy or their own edge.

## systemd user unit

`systemd/selvaged.service` is the Pi's hand-written unit, generalized for any
machine: absolute binary path, explicit `--listen`, explicit `--room-grace-ms`,
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
curl -sS http://<your-address>:8080/meta   # answers selvaged/<version> with selvage/2
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
binary. Build locally — the build fetches the page's pinned `web_client`
revision, so it needs network, and running it needs no mount:

```sh
docker buildx build -t selvaged:local .
docker run --rm -p 127.0.0.1:8080:8080 selvaged:local
curl -sS http://127.0.0.1:8080/meta
curl -sS http://127.0.0.1:8080/ | head -c 200   # the page, from the image
```

Primary shape is a musl-static binary on `scratch`: the lockfile carries no
TLS C shims (no openssl/ring/aws-lc-sys — verified by grep over `Cargo.lock`;
only `libc`/`mio` as OS shims), so the static build needs no C libraries, and
the runtime holds one binary plus `/LICENSE` as non-root user 65532, no
shell, no package manager. The `Dockerfile` cross-compiles both
architectures natively on an x86\_64 builder (one cross-toolchain stage per
target, each pinned `--platform=$BUILDPLATFORM`), so building arm64 needs
no QEMU — only *running* the arm64 image does. Build and run it on any
machine with a working Docker; the `container` job below does exactly that on
GitHub-hosted `ubuntu-24.04` (the amd64 path), and the `publish-rehearsal` job
builds both architectures there with buildx, so the `Dockerfile` is CI-proven
as well as human-verified.

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
published releases; `scripts/release-tags.sh` computes the same identity for
the two buildx jobs and refuses a release tag whose name is not the Cargo
version, so a tag cut from the wrong commit cannot publish a mismatch.
`--version`, `/meta`, and `CARGO_PKG_VERSION` are already
wired together in code (`Meta::reference`, test-enforced by
`version_matches_what_meta_serves`); `scripts/check-server-version.sh`
asserts the same truthfulness against a running server, and
`scripts/image-smoke.sh` runs the whole smoke without publishing and
without Docker: it builds `packages.image` (the nix/dockerTools image
below), verifies manifest and container config with skopeo, extracts the
exact binary from the image layers, and checks `--version` and `/meta`
against it. `scripts/ci-local.sh image` runs that smoke anywhere nix does.

### Published releases

The package holds `0.1.0-7d64cbb`, `0.1.0`, `0.1.1-a3f4b12`, `0.1.1`, `0.1.2-adc0ae2`, `0.1.2`,
`0.2.0-67c3b7d`, `0.2.0`, `0.2.1-030615c`, `0.2.1`, `0.4.0-1a12e2f`, `0.4.0`, and the moving
`latest`, which is `0.4.0` as this is written (2026-09-24).
`v0.1.0` (2026-09-19, at `7d64cbb`, the merge of \#25) was the first: its tag run —
https://github.com/selvage-protocol/reference\_server/actions/runs/35428735910 —
published `ghcr.io/selvage-protocol/selvaged:0.1.0-7d64cbb`, `:0.1.0` and `:latest`
from the `publish` job.

**The package is public, and an anonymous pull of it works.** The registry mints a pull
token to a caller who names no identity; verified on 2026-09-21 with **no credentials
presented**, the tag list and a blob pull both answering. The owner set that, and it is the
package's own visibility setting, which is irreversible; it is separate from the
organization's **Package Creation** setting, which decides whether members may create
public packages at all.

**`0.1.0` and `0.1.1` carry the amd64 binary in their `linux/arm64` leg**, so the tag says
nothing about the architecture inside it: those two are for `linux/amd64`, and an ARM
machine builds from a checkout. `0.1.2` is the first release whose legs are separate
builds. `scripts/assert-multiarch-layers.py <tag>` reads a tag's two platform manifests out
of the registry and checks each one's `/selvaged` for the ELF machine its platform claims.
Against `0.1.0` and `0.1.1` it reports this and exits non-zero:

```text
linux/amd64 and linux/arm64 carry the exact same layer digests: the arm64 leg was not built separately from the amd64 one
linux/arm64: /selvaged has ELF e_machine 62, want 183
```

and against `0.1.2`:

```text
layer digests OK: amd64 and arm64 differ
ELF OK on linux/amd64: e_machine 62
ELF OK on linux/arm64: e_machine 183
multiarch OK: ghcr.io/selvage-protocol/selvaged:0.1.2 carries a genuine binary per platform
```

`publish` and `publish-rehearsal` both run that assertion, after the push: a tag whose legs
are the same build is already on GHCR when the job goes red, which is how `0.1.0` and
`0.1.1` were published and what `0.1.2` fixed.

A stranger pulls it with no account:

```sh
docker run --rm -p 127.0.0.1:8080:8080 ghcr.io/selvage-protocol/selvaged:0.4.2
```

and `compose.yaml`'s `build: .` remains the route for an image built from a checkout.

### Compose

`compose.yaml` is one service and one port mapping, and it runs the image the
way the hosting gate requires (owner-decided 2026-09-19): `read_only: true`,
`cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`, and nothing
mounted — no host socket, no volume, no writable path, which the server does
not need because it keeps nothing on disk. `docker compose up`, then
`curl /meta`, then host a room from an editor. The page is the image's own; a
page built elsewhere is served by uncommenting the single `./page` mount the
file documents (compose mounts unconditionally when a volume is set, and an
empty directory would replace the baked page, so the override is commented
rather than absent). `image:` names the tag the release publishes and `build: .`
builds the same Dockerfile locally, so `compose up` works from a checkout whether
or not that tag is pullable yet.

### One origin: the page

`selvaged --serve-page DIR` serves the browser page on the same origin as
`/session` and `/meta` (see the root README). The image bakes the page at
`/page` and its default command passes `--serve-page /page`, so one container
and one port answer the page, the meta document and the socket, and a
self-hoster needs no CORS proxy and no second page server. The Pi's native shape
did the same thing behind its TLS front (`pi-demo/`). Neither deployed shape does
it: `prod/` and `pi/` each run the page as its own container and turn the baked
page off, which is why both name a page image beside the server image.

**The page is built into the image.** The `web_client` bundle is not vendored
here: the `Dockerfile`'s `page` stage clones that repository at the revision in
the `WEB_CLIENT_SHA` build argument, runs `npm ci && npm run build` on a Debian
trixie node image (the client's icon renderer shells out to ImageMagick 7's
`magick`), and copies the resulting `dist/` to `/page`. The pin is
`609eac29f77886fc0effdfa7eaa34827d155b174`, the commit `web_client`'s `main`
names for its `0.4.2`; updating it is editing that one argument, and the image
records the revision it carries in `com.selvage.page.revision`. The page is
built from that revision's source rather than copied from the `dist/`
committed there; `web_client`'s own `checks` job rebuilds that `dist/` on every
pull request and compares the two with `scripts/check-dist.sh`, which is why the
bytes are the reviewed ones and why the page cannot fall behind its source.
The page the image serves must speak the wire this server seats, since one built
from a revision that names another wire cannot join the container beside it: the
pin therefore names a revision whose bundle names `selvage/2`, which is what
`scripts/container-smoke.sh` asserts of the served bundle. It is a revision of
another repository, so it moves when *that* repository releases, and a wave that
cuts `web_client` and this repository together repins it to the released commit
before this image is cut, because the page inside the image is only as new as
the revision named here.

Overriding the baked page is a mount over `/page` (`-v …/dist:/page:ro`), and it
needs no command override: the image's own command already names that directory.
A missing `./page` is not fatal either way — `/meta` and `/session` still
answer and `/` is a `404`.

### A page-only image, for a second origin

`web_client` publishes the page by itself as
`ghcr.io/selvage-protocol/selvage-web`; that repository's README owns its build, its tags,
its runtime and its CI. Use it when the browser client should live on an origin of its own
— one page in front of several `selvaged` instances, or a page host separate from the
servers — instead of sharing the server's one listener.

Two things follow from that, and they are the honest cost. The page becomes a **second
origin**: the socket is not CORS-bound and the `/meta` read is advisory, so a cross-origin
page works, but a link at that origin carries no room: the page reads the server from the
link's own address and from nowhere else, so a guest there is handed the wire shape
(`ws://HOST:PORT/session?room=…&token=…`) rather than a page link.
And a one-origin deployment with the page, `/meta` and `/session` on one port needs neither,
so **one origin stays the default** — the shape above, and the single service in
`compose.yaml`.

### TLS

`selvaged` speaks no TLS and claims none: an invite over `ws://` is plaintext,
and an `https` page cannot dial a `ws://` socket (mixed active content). For a
shareable link, put a terminator in front — `tailscale serve`, caddy, or your
edge — and hand out the `https://`/`wss://` URL. Serving the page from the same
origin as the socket is what makes that one terminator enough.

## CI

`.github/workflows/image.yml` has five jobs. `smoke` runs on a pull request
that changes something the image is built from — `crates/`, the manifests, the
`Dockerfile`, `compose.yaml`, `packaging/`, the flake, and the scripts a job
reads — and on a release tag, one leg per architecture (`amd64` on
`ubuntu-24.04`, `arm64` on `ubuntu-24.04-arm` — each building natively, no
cross-compilation): the same nix preamble as the checks job, then
`scripts/image-smoke.sh`, which needs no Docker daemon at all: it builds the
image with `nix build .#image` and verifies the manifest and container config
with skopeo. The `Dockerfile` stays the portable static variant for
machines with a working Docker; both agree on entrypoint, port, user,
licence label and tag scheme, and the smoke asserts exactly those fields. The
nix image is the daemon-free shape and carries the server alone: the page is the
`Dockerfile`'s own, and `container` is where it is proved.

`container` is the other half, and the only job that runs the image, on one
architecture: GitHub-hosted `ubuntu-24.04`, chosen for its Docker daemon and
its full egress, a plain `docker build` of the `Dockerfile`, and
`scripts/container-smoke.sh`. That script
asserts
`compose.yaml` carries the hardened run, that the container actually runs that
way (read-only root filesystem, `CapDrop: [ALL]`, `no-new-privileges`, no mount
at all), that the container's `--version` and `/meta` agree with `Cargo.toml`,
that the page baked into the image is served with the headers the static
handler pins, and then mints a room in the container and joins it as a guest
with the harness's client (`crates/harness/examples/join_room.rs`), receiving
the host's edit. A `--version` or `/meta` check is not that proof: no
client had ever completed a handshake against the image.
`scripts/ci-local.sh container` runs the same script where a Docker daemon and
the compose plugin exist.

`publish-rehearsal` is the publish path with nothing published: GitHub-hosted
`ubuntu-24.04`, `docker/setup-qemu-action`, `docker/setup-buildx-action`, a
`registry:2` container on the runner's own loopback, the same
`scripts/release-tags.sh`, the same multi-architecture buildx build as
`publish` — moving `<version>`/`latest` aliases included — and the same
`scripts/assert-image-version.sh` and `scripts/assert-multiarch-layers.py`
assertions: `--version` on both architectures, `/meta` against the image that
was built, and each platform's binary checked for the ELF machine it claims. It
holds no GHCR credential and no elevated permission, and nothing leaves the
runner.

`publish` needs `[smoke, publish-rehearsal]`, carries an elevated
permission (`packages: write`), logs in to GHCR with the
built-in `GITHUB_TOKEN`, and is gated on a tag push (`refs/tags/v*`) whose
name must equal the Cargo version. It builds with Docker while `smoke` builds
with nix, which is why the rehearsal exists and why the publish path is
re-proven on every pull request before a tag can reach it. `release` needs
`[publish]`, carries `contents: write`, and is
gated the same way: the ref is a `v*` tag, not a particular triggering event
(below). It creates the GitHub Release for the tag from
`scripts/release-notes.sh` once the image is pushed and re-asserted, and does
nothing on a re-run when the release already exists.

`.github/workflows/release.yml` is the button: a `workflow_dispatch` that takes
a version input, checks it against Cargo.toml with the same `scripts/release-tags.sh`
the tag is checked with again above, refuses an input whose tag already exists
on the remote, creates and pushes that tag, then asks the API to dispatch
`image.yml` against it. That last step is why `publish` and `release` above
gate on the ref rather than on `github.event_name == 'push'`: GitHub does not
start a new workflow run for an event a workflow produced with its own
`GITHUB_TOKEN`, and a tag this workflow pushes is exactly such an event, so it
would reach `image.yml` unpublished without an explicit dispatch —
`workflow_dispatch` is the one event that rule exempts. A dispatch run still
cannot publish an image or create a release for a commit that is not a
matching tag: `release.yml` has no route to GHCR or to a GitHub Release itself,
and `image.yml`'s own `workflow_dispatch` (no inputs) reaches the gate only
when it is pointed at a ref that already is one. A pull request cannot reach
either workflow's `workflow_dispatch` at all.

### The page in a container

The image carries the page and serves it, so the container smoke's first
subject is the baked page: `/` is the shell (`text/html`, `no-cache`,
`Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`, the content
security policy), `/app.js` is the bundle, and a content-hashed chunk the bundle
itself names is served `public, max-age=31536000, immutable`. The extraction
fails the smoke when it finds no such name, so a report of a clean page cannot
come from having read none. Those headers are the static handler's own
(`crates/selvaged/src/page.rs`) and pinned in `crates/harness/tests/page.rs` as
well, so the container job proves them where they are actually served rather
than only in-process. A second container then mounts a three-file page over
`/page` with no command override and asserts the same policy plus the exact
bytes, which is the override `compose.yaml` documents.
