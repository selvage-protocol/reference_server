# selvage-reference-server

A Rust workspace proving the Selvage session protocol end to end: a memory-only
reference server, a client library with an editor-adapter seam, and a headless
two-client harness that gates it in CI.

## Get it working

Three routes to a running `selvaged`: from this checkout, from the container image, or
through `nix run`. It is one binary with no configuration file, and the flags below are
the whole of its surface.

### From a checkout

`cargo` and `rustc` are not on this host's ambient PATH; they come from the flake, so run
cargo through the dev shell:

```sh
nix develop . -c cargo run -p selvaged -- --listen 127.0.0.1:8080
```

Without Nix, [install Rust via rustup](https://www.rust-lang.org/tools/install) (any
recent stable works), and the same `cargo run` needs no other setup. For a binary to keep,
build it release and run that:

```sh
nix develop . -c cargo build --release --locked -p selvaged
./target/release/selvaged --listen 127.0.0.1:8080
```

### The container

One image serves the page and the server on one port, built from this checkout:

```sh
docker buildx build -t selvaged:local .
docker run --rm -p 127.0.0.1:8080:8080 selvaged:local
```

`docker compose up` builds the same `Dockerfile` through `compose.yaml`, which runs the
image read-only with every capability dropped. The published image is
`ghcr.io/selvage-protocol/selvaged:0.1.0`, with `:latest` the same build. That GHCR package
is private at the time of writing, so pulling it works only for the account that owns it,
and the local build is the route that works for everyone.

The page is baked in. The image builds the browser client's `dist/` from a pinned revision
and serves it, so a container needs no mount. A page built elsewhere overrides the baked
one by mounting over it, because the image's own command already passes `--serve-page
/page`:

```sh
docker run --rm -p 127.0.0.1:8080:8080 \
  -v "$PWD/page:/page:ro" \
  selvaged:local
```

With no page directory the server still answers `/meta` and `/session`, and `/` is `404`.
`packaging/README.md` is the rest of it: tags and version truthfulness, the multi-arch
build, the FSL-1.1-MIT redistribution question, the systemd user unit, and the Pi demo.

### Nix

The flake's default package is `selvaged`, so `nix run` builds the binary and starts it
with the same flags:

```sh
nix run . -- --listen 0.0.0.0:8080
```

### What it prints, and its flags

The startup line names the endpoint and the meta address:

```text
selvaged listening on ws://127.0.0.1:8080/session (meta at http://127.0.0.1:8080/meta)
```

With the default bind it goes on to note that the address is loopback-only and how to
widen it, how long rooms live after their host disconnects, and that a client mints the
room. `--help` prints the usage line, a short description of the server, and every flag
with its default.

```text
usage: selvaged [--listen ADDR] [--room-grace-ms MS] [--serve-page DIR]
```

| Flag | What it does |
|---|---|
| `--listen ADDR` | bind `ADDR` (default `127.0.0.1:8080`) |
| `--room-grace-ms MS` | how long a room survives its host disconnecting, in milliseconds (default `30000`, which the help prints as 30s) |
| `--serve-page DIR` | serve the browser page from `DIR` on the same origin as `/session` and `/meta` |
| `--help`, `-h` | print the usage and the flags |
| `--version` | print `selvaged/<version>` |

### The first room

The server mints nothing to share. It holds rooms in memory and waits for a connection;
the client that hosts mints the room and prints the invite link. The invite is the
permission, and the whole of it:

```text
ws://HOST:PORT/session?room=<room>&token=<token>
```

Anyone holding that link can join until the room dies. Host from an editor with
[`vscode_client`](https://github.com/selvage-protocol/vscode_client) or
[`nvim_client`](https://github.com/selvage-protocol/nvim_client); the
[`web_client`](https://github.com/selvage-protocol/web_client) page joins one as a guest.
A client connected to your server is what mints a room for a friend, not the harness
transcript.

### Letting someone else in

The default binds loopback, so only your own machine reaches it. For a friend to join, bind
an address they can reach and hand them a URL that names your machine:

```sh
nix develop . -c cargo run -p selvaged -- --listen 0.0.0.0:8080
```

A free tunnel that forwards to your port works for a first test. Beyond that you want a
machine with a public address (a small VPS, and the right firewall rules).

Plain `ws://` and `http://` is plaintext: the invite token travels in the clear, so it is
for a tailnet, a VPN or loopback. The protocol's transport security is the deployer's to
supply; put a TLS terminator in front (`tailscale serve`, caddy, or your edge) and hand
out the `https://` or `wss://` URL. There is no TLS inside `selvaged`, and none is
claimed.

### The checks worth running

Three commands, and they do different things:

```sh
nix develop . -c cargo test                                   # protocol unit tests, convergence and lifecycle
nix develop . -c cargo test -p selvage-harness --test vectors  # the vector replay over vectors/
nix develop . -c cargo run -p selvage-harness                  # the whole slice, printed step by step
```

`cargo run -p selvage-harness` runs a scripted demo transcript: it starts a server, mints
a room, prints the invite link, walks two clients through it, and exits. `selvaged` waits
for a client to connect instead, and a client's output carries the invite link.

`scripts/ci-local.sh` runs the same commands as `.github/workflows/ci.yml` on this machine,
one flake check per step, and needs `nix`:

```sh
scripts/ci-local.sh all        # format, clippy, tests, the default package, licences, every check evaluated, actionlint
scripts/ci-local.sh nightly    # coverage, the rest of cargo-deny and cargo-audit (slow)
scripts/ci-local.sh lint       # actionlint over the workflow files, on its own
scripts/ci-local.sh image      # the nix-built image smoke, no Docker needed
scripts/ci-local.sh container  # docker build, docker run and a room join (needs Docker)
```

`all` is what the `checks` job runs; `nightly` is opt-in because it is slow. Two jobs of
the `image` workflow have no step here, the multi-arch buildx rehearsal and publish, since
this host has no Docker, let alone buildx; they are read from the run and from the scripts
those jobs share with `container`.

## What lives here

| Path | What it is |
|---|---|
| `crates/protocol` | `selvage/1` session envelope, method/event/error vocabulary, invite URLs. No I/O. |
| `crates/selvaged` | the server: rooms, membership, the open-document set, payload-opaque relay, `GET /meta` |
| `crates/client` | the sync engine (one `Y.Doc` per session, one `Y.Text` per document, y-protocols, awareness) and the `EditorAdapter` seam |
| `crates/harness` | one server plus N clients, driven programmatically; also a runnable transcript, and the vector replay over `vectors/` |
| `vectors/` | the wire vectors, vendored from the specification; `scripts/sync-vectors.sh` refreshes them |

## The server's shape

`selvaged` serves `ws://HOST:PORT/session` and `http://HOST:PORT/meta` on one listener. It
keeps nothing on disk: the rooms are in memory, so keep the process running, because Ctrl-C
ends all of them and a restart ends every room. A room outlives its host's lost connection
for the grace period (`--room-grace-ms`, 30 seconds by default); rejoining with the same
invite link inside that window reclaims the room.

A room holds its membership, the open-document set, and the peers seated in it. It holds
nothing about what those peers say: no part of the server reads a document payload or an
awareness payload, and the relay between peers is opaque to both. A peer's role is `host`
or `guest`, the invite token is the permission, and the room dies with its host. There are
no accounts and no file access.

## The client library and the harness

`crates/client` is the sync engine (one `Y.Doc` per session, one `Y.Text` per document,
with y-protocols and awareness behind it) and the `EditorAdapter` seam an editor implements.
`crates/harness` puts one server beside N clients driven programmatically, and it is also
where the runnable transcript and the vector replay live.

## GET /meta

`GET /meta` answers a JSON body with the server string (`selvaged/<version>`, the same one
`--version` prints), the wire versions it speaks (`selvage/1`), the roles it seats (`host`,
`guest`), its capabilities, and the keepalive and room-grace values it is configured with.

## Serving the page

`selvaged --serve-page DIR` serves the browser page from the same origin as `/session` and
`/meta`. One process, one port, one origin: the page's `/meta` read is same-origin, so it
needs no CORS proxy, and its socket is `ws://` or `wss://` on the page's own host, so there
is no cross-origin dial. The guest link is then a page link,
`http://HOST:PORT/?room=<room>&token=<token>`, with no `server=` parameter.

The page is the browser client's built `dist/`, which lives in the `web_client` repository;
the image builds it from a pinned revision and serves it.

Served files carry the policy a browser needs: a media type from a pinned table (never the
host's mime database), `Cache-Control: no-cache` for a stable name and `public,
max-age=31536000, immutable` for a content-hashed one, `Referrer-Policy: no-referrer`
because an invite URL carries the room token and must not travel on in a `Referer` header,
`X-Content-Type-Options: nosniff`, and a `Content-Security-Policy`.

`scripts/container-smoke.sh` builds the image with Docker, runs it read-only with every
capability dropped, asserts the page the image bakes, and joins a room in it with the
harness's client engine; `scripts/ci-local.sh container` runs the same where a Docker daemon
exists.

## The vectors

The wire protocol is described by the specification repository,
[`selvage-protocol/specification`](https://github.com/selvage-protocol/specification):
`PROTOCOL.md` is the prose, `CANONICAL.md` fixes the bytes of a frame, and `schema/` is the
machine-readable model. That repository is the canonical source for all three, and this one
authors none of them.

Its wire vectors are vendored here as `vectors/`, next to the harness that replays them, so
that a plain `cargo test` and the Nix sandbox need no sibling checkout.
`scripts/sync-vectors.sh` copies them in from a specification checkout when they change; the
specification remains the canonical source.

The replay reads `vectors/`, or `SELVAGE_VECTORS` when that is set. The Nix build cannot see
outside the Cargo workspace, so `flake.nix` hands the directory in explicitly.

## What this slice does not do

No persistence, no accounts, no file access, no read-only guests, no E2EE, no editor
integration. `PROTOCOL.md` §12 lists every decision the design record leaves open.

## Licence

The server is licensed differently from everything beside it.

| Path | Licence |
|---|---|
| `crates/protocol`, `crates/client`, `crates/harness` | `MIT OR Apache-2.0`, the workspace default: [`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE) |
| `crates/selvaged` | `FSL-1.1-MIT`: [source-available, *not* open source](crates/selvaged/LICENSE) |
| `vectors/` | vendored from the specification repository, whose material is `CC-BY-4.0` |

`crates/selvaged/Cargo.toml` carries `publish = false`, and `cargo deny check licenses` is
told about `FSL-1.1-MIT` for that one crate. The Functional Source License 1.1 is free for
any non-competing purpose: a company self-hosting it internally is free, as are
non-commercial education and research. It forbids making the software available to others in
a commercial product or service that substitutes for it. A free competing relay is not a
Competing Use under that text. Each release converts to MIT on the second anniversary of the
date it was made available, irrevocably.

The harness links `selvaged`, so its own `MIT OR Apache-2.0` covers the crate while a
redistributed `selvage-harness` binary carries FSL code with it.

The vendored vectors are `CC-BY-4.0`
([`selvage-protocol/specification`](https://github.com/selvage-protocol/specification)), which
this repository does not author and redistributes with that repository as the source of the
attribution.
