# selvage-reference-server

A Rust workspace proving the Selvage session protocol end to end: a memory-only
reference server, a client library with an editor-adapter seam, and a headless
two-client harness that gates it in CI.

## Get it working

`selvaged` is one binary with no configuration file, and the flags below are the whole of
its surface. The shortest route to a running server is the published image:

```sh
docker run --rm -p 127.0.0.1:8080:8080 ghcr.io/selvage-protocol/selvaged:0.2.1
```

The GHCR package is public, so a pull needs no account and no `docker login`. `0.2.1` is
the current release; `0.1.2` was the first built separately for each architecture, and
`0.1.0` and `0.1.1` carry the amd64 binary in their `linux/arm64` leg.
`packaging/README.md` owns the full tag list and what each tag contains.

### From a checkout

With Nix, `cargo` and `rustc` come from the flake, so run cargo through the dev shell:

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

### Build the image from this checkout

One image serves the page and the server on one port:

```sh
docker buildx build --load -t selvaged:local .
docker run --rm -p 127.0.0.1:8080:8080 selvaged:local
```

`docker compose up` builds the same `Dockerfile` through `compose.yaml`, which runs the
image read-only with every capability dropped. This is the route for an image you are
changing, or one built from your own checkout.

The page is baked in, so a container needs no mount, and a page built elsewhere overrides
it by mounting over `/page` — the image's own command already passes `--serve-page /page`:

```sh
docker run --rm -p 127.0.0.1:8080:8080 -v "$PWD/page:/page:ro" selvaged:local
```

The container serves as UID `65532`. Mounted page files need read permission, and their
directories need read and search permission for that UID, through ownership, group
membership, or mode bits. Inaccessible files return `404`. With no page directory the
server still answers `/meta` and `/session`, and `/` is `404`.

`packaging/README.md` is the rest of it: tags and version truthfulness, the multi-arch
build, the FSL-1.1-MIT redistribution question, the systemd user unit, and the Pi demo.

### Nix

The flake's default package is `selvaged`, so `nix run` builds the binary and starts it
with the same flags:

```sh
nix run . -- --listen 0.0.0.0:8080
```

### What it prints, and its flags

The startup line names the endpoint and the limits this process is enforcing:

```text
selvaged listening on ws://127.0.0.1:8080/session (meta at http://127.0.0.1:8080/meta)
limits: 1024 connections, 1024 rooms, 128 peers per room, 1024 documents per room, 32 MiB outbound per connection, 5 MiB inbound text envelope, 2 MiB/s inbound with a 64 MiB burst
```

With the default bind it goes on to note that the address is loopback-only and how to
widen it, how long rooms live after their host disconnects, and that a client mints the
room. `--help` prints the usage line, a short description of the server, and every flag
with its default.

```text
usage: selvaged [--listen ADDR] [--room-grace-ms MS] [--serve-page DIR]
                [--max-connections N] [--max-rooms N] [--max-peers-per-room N]
                [--max-documents-per-room N] [--outbound-queue-bytes N]
                [--max-envelope-bytes N] [--inbound-bytes-per-sec N]
                [--inbound-burst-bytes N] [--serve-version-1-only]
```

| Flag | What it does |
|---|---|
| `--listen ADDR` | bind `ADDR` (default `127.0.0.1:8080`) |
| `--room-grace-ms MS` | how long a room survives its host disconnecting, in milliseconds (default `30000`, printed as 30s) |
| `--serve-page DIR` | serve the browser page from `DIR` on the same origin as `/session` and `/meta` |
| `--serve-version-1-only` | seat `selvage/1` alone. Every version is seated by default, so `/meta` advertises both and a `selvage/2` connection is seated; this narrows the server to the version-1 corpus's own shape, where a `selvage/2` hello is refused `unsupported_version` (close `4005`). A room is pinned to the version its minting connection spoke either way |
| `--max-connections N` | connections held at once, counted past the request head (default `1024`) |
| `--max-rooms N` | rooms held at once; past it a room is not minted (default `1024`) |
| `--max-peers-per-room N` | peers one room seats at once (default `128`) |
| `--max-documents-per-room N` | paths one room's open-document set holds (default `1024`) |
| `--outbound-queue-bytes N` | payload bytes queued but unwritten for one connection before it is dropped as a peer that stopped reading (default `33554432`, 32 MiB; the command line refuses one that cannot hold the largest frame this configuration can generate, which is never below one whole frame, `8388608`) |
| `--max-envelope-bytes N` | the largest inbound text envelope the server will parse, judged on the frame's length before `serde_json` sees it (default `5242880`, 5 MiB) |
| `--inbound-bytes-per-sec N` | bytes one connection may send a second, refilled continuously (default `2097152`, 2 MiB) |
| `--inbound-burst-bytes N` | how much of that rate one connection may spend at once (default `67108864`, 64 MiB) |
| `--help`, `-h` | print the usage and the flags |
| `--version` | print `selvaged/<version>` |

Every default is the reference value. The four capacity flags are bounds on what one
process holds in memory and only that process can enforce them; the envelope bound and
the inbound budget are what one connection may send, and they are judged in-process
because a front cannot see either one. *Sizing a box*, below, says which of a
deployment's bounds belong where.

### What happens at a limit

Every limit refuses deterministically, and what a peer sees depends on the limit:

| Limit reached | What the peer sees |
|---|---|
| `--max-connections` | a plain HTTP request is answered `503` with `retry-after`; a WebSocket upgrade is answered and then closed `1013` |
| `--max-rooms` | a refusal, `session.error` with code `x.server_full`, then close `4000` |
| `--max-peers-per-room` | `x.room_full` on the join, then close `4000`. A host reclaiming a host-less room is always seated |
| `--max-documents-per-room` | an error response `x.room_full` to the `doc.open`; the connection stays open and the set is unchanged |
| `--max-envelope-bytes` | a seated connection gets `session.error` `bad_message` naming the bound and the frame's size; the connection stays open. Before the handshake the same refusal closes `4000` |
| `--inbound-bytes-per-sec` / `--inbound-burst-bytes` | `session.error` `x.rate_limited` naming the budget, then close `1013`; the room is told `peer.left` and a reconnect starts with a fresh budget. Before the handshake the same refusal closes `4000`, since every fault before seating closes |
| `--outbound-queue-bytes` | the peer is disconnected as one that stopped reading, and the room is told `peer.left` |

The rate limit is charged per inbound frame — the handshake's frames included, which is
where a peer can send frames nobody answers — at its payload size or one kilobyte,
whichever is larger: a flood of one-byte frames costs a kilobyte of budget each, because
that is closer to what a frame costs the server than its payload is. Frames the session
never parses — relayed document and awareness payloads — are charged too, since a relay is
copied once per peer. A refusal is written before the socket closes; a peer that is still
writing when the server closes it can lose that refusal to a reset its own writes bring,
which is the same drop `PROTOCOL.md` §2.1 describes for an over-bound frame.

### Sizing a box

A small host should size the process rather than trust the defaults, which are the
reference values and assume headroom. `max_connections` multiplies the per-connection
outbound queue, so `connections × outbound-queue-bytes` is the outbound ceiling the
process can reach (at the defaults, 32 GiB), and `--max-connections` is what a small box
lowers first. The document set and the grant are per-room state, and the inbound budget
bounds what one connection can spend of the CPU.

The queue is also the floor under every frame the server sends, and it is refused at
startup if it cannot hold one. The largest frames are a relayed payload (the frame bound,
8 MiB), the room's open-document set echoed to every peer on every `doc.open`/`doc.close`,
and the whole grant that host published — and what is counted is the frame, not the path it
names. JSON writes a `"` or a `\` as two bytes, so `--max-documents-per-room` paths of
4 KiB each are twice their bytes on the wire when they are written in either, and a queue
counted in path bytes is not a floor. A queue below the largest of those does not bound
memory, it breaks sessions — a handshake frame nobody can queue seats nobody, and every
peer is ejected, the publisher included, for a `doc.open` the server itself echoed — so the
command line refuses the combination and says which flag to move. That also makes the queue
the place the echo is paid for: the set is why lowering `--max-documents-per-room`
is what buys a smaller queue.

For a 1 GiB box with something else running on it, these are a defensible set:

```sh
selvaged --listen 0.0.0.0:8080 --serve-page /page \
  --max-connections 32 --max-rooms 64 --max-peers-per-room 8 \
  --max-documents-per-room 256 --outbound-queue-bytes 8388608
```

That is an outbound ceiling of 256 MiB, not a figure anything reaches in a session: it is
every one of 32 connections holding a full queue of unwritten frames at once, which is what
the queue's own cap ejects. The two inbound bounds keep their defaults here: 5 MiB is what
a `doc.grant` of ordinary paths needs, a listing whose paths all escape carries half the
path bytes it otherwise would and is refused with the bound named, and 2 MiB/s is already
far above what an editor sends.
Lowering `--inbound-bytes-per-sec` below a few tens of kilobytes a second will exile a peer
for traffic it did not choose to send: a client publishes presence on a timer, and each of
those frames costs a kilobyte of budget.

`selvaged` does not implement an idle deadline, and `PROTOCOL.md` §2.1 forbids closing a
seated session for silence: a connection that answers its pings is never closed for being
quiet. A deployment that needs one puts it in front, and the half of it that is in-process
already is `head_timeout` (5 s): a connection that does not finish sending its request head
inside that is closed, which is what reclaims a half-open socket before it is ever counted
against `--max-connections`.

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

The harness is the quickest way to watch the protocol work:

```sh
nix develop . -c cargo run -p selvage-harness                  # the whole slice, printed step by step
nix develop . -c cargo test                                    # protocol unit tests, convergence and lifecycle
nix develop . -c cargo test -p selvage-harness --test vectors  # the vector replay over vectors/
```

`cargo run -p selvage-harness` runs a scripted demo transcript: it starts a server, mints
a room, prints the invite link, walks two clients through it, and exits. `selvaged` waits
for a client to connect instead, and a client's output carries the invite link.

`scripts/ci-local.sh` runs the same commands as `.github/workflows/ci.yml` on this machine,
one flake check per step, and needs `nix`:

```sh
scripts/ci-local.sh all        # the whole gate, and what the `checks` job runs
scripts/ci-local.sh nightly    # coverage, the rest of cargo-deny and cargo-audit (slow)
scripts/ci-local.sh lint       # actionlint over the workflow files, on its own
scripts/ci-local.sh image      # the nix-built image smoke, no Docker needed
scripts/ci-local.sh container  # docker build, docker run and a room join (needs Docker)
```

`all` is what the `checks` job runs; `nightly` is opt-in because it is slow. The `image`
workflow's two buildx jobs have no step here, since this host has no Docker, let alone
buildx; a pull request's checks are where they run.

## What lives here

| Path | What it is |
|---|---|
| `crates/protocol` | `selvage/1` session envelope, method/event/error vocabulary, invite URLs. No I/O. |
| `crates/selvaged` | the server: rooms, membership, the open-document set, payload-opaque relay, `GET /meta` |
| `crates/client` | the sync engine (one `Y.Doc` per session, one `Y.Text` per document, y-protocols, awareness), the `EditorAdapter` seam, and `selvage/2`: the sealed frame (`sealed.rs`), the peer session (`peer.rs`), the host's producer half (`host.rs`) and the relay that puts a session on a socket (`relay.rs`) |
| `crates/harness` | one server plus N clients, driven programmatically; also a runnable transcript, the vector replay over `vectors/`, and `selvage-subject`, the client the peer corpus's decision layer drives |
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
or `guest`, the invite token is the permission, and the room is removed after the grace
period if its host does not reclaim it. There are no accounts and no file access.

### `selvage/2`, seated beside `selvage/1`

One process seats both wire versions by default: the published clients speak `selvage/1`,
and `selvage/2` is what the revision's clients speak, so `/meta` advertises both and a room
serves whichever version its connections speak. A room is pinned to the version its
**minting connection** spoke — a version-1 room expects the server to hold the document set
and a version-2 one requires that it does not, so one room cannot serve both — and a
connection that speaks the other version is refused `unsupported_version` with close
**4005** before it is seated (`PROTOCOL.md` §10). `--serve-version-1-only` narrows the
process to `selvage/1` alone, which is the shape the specification's version-1 corpus pins
and what a client that has to be shown the refusal is pointed at. Nothing else changes for
a version-1 client: the same methods, the same events, the same refusals.

A version-2 room is smaller than a version-1 one. The server records membership — a
`peer_id`, a display name and an awareness client id per connection — and it holds no
host, no open-document set, no grant and no document of any kind. Its method surface is
`session.hello` and `session.rename`; a `doc.*` request is `unknown_method` and the
connection stays open. A binary frame is a sealed frame the server relays byte for byte
and cannot read, and no frame it authors carries a path, a role or a document name. The
room's life is its **last** connection rather than its host's: the grace timer arms when
the room's last connection ends, and the destruction has no recipient, so it is silent and
the next connection that names the id is told `room_unknown`.

`selvage/2`'s peer side — the sealed frame, the room state, the holds and the client's own
rules — is specified in `PROTOCOL.md` §7.1 and §13. This server's part of it is only the
relay and the membership.

The client's part of it is `crates/client/src/peer.rs`: `crates/client/src/sealed.rs` is
`CANONICAL.md` §6.1's bytes, and `peer.rs` is `PROTOCOL.md` §13 on top of them — the session
keypair and its announcement, the order of operations at a join, what may be published
before and after a state commits the connection's key, attribution by the key that verified,
the holds and their lease, and the two windows that end a session. It holds no socket: a
frame goes in, the decisions come out, and every clock is a value the caller passes in, which
is what lets the corpus drive it.

Around it are the two halves it was written to be handed. `host.rs` is §7.1's producer — the
room state's listing, roles and `issued` series, and the `HostStore` a host that means to keep
hosting keeps its key and its series in — and `relay.rs` is the connection: it opens the
WebSocket, says `session.hello` at `selvage/2`, seats the session from
`room.created`/`room.joined`, hands every binary frame to it and every frame it produced to the
socket, and runs its clocks on a timer of its own. §5.1's two invite forms are one reading: a
link whose fragment carries `k` and `h` resolves to a sealed invite through
`ConnectOptions::from_invite_url` and is joined with `relay::RelaySession`, and a link without
one stays `selvage/1`. A fragment that is present but is not both keys is refused where the
link is read — `ConnectOptions::read_invite_url` is that reading, and its error says which
value is wrong — rather than dialled as the other version, whose socket URL would carry the
fragment. `crates/harness/tests/relay_selvaged.rs` is that pair against a real `selvaged`.

Two suites hold that layer. `crates/harness/tests/peer_vectors.rs` replays the corpus's
nineteen **frame** vectors against the sealed layer, and `crates/harness/tests/decisions.rs`
drives its six **decision** vectors against the client through `selvage-subject`, the binary
that speaks the corpus's subject protocol
(`cargo run -p selvage-harness --bin selvage-subject`). The specification's own runner can
drive the same binary:

```
python3 runner/run_peer.py --subject <checkout>/reference_server/target/debug/selvage-subject
```

All six decision vectors pass, and each goes red under the guard it declares it catches: the
same suite removes the one guard a vector names and shows the vector fail, so a rule vector
cannot pass by asserting nothing.

## The client library and the harness

`crates/client` is the sync engine (one `Y.Doc` per session, one `Y.Text` per document,
with y-protocols and awareness behind it) and the `EditorAdapter` seam an editor implements.
It reads `GET /meta` best-effort before the first socket, so a reconnect's budget spans the
room grace the server advertises (`PROTOCOL.md` §9.1).
`crates/harness` puts one server beside N clients driven programmatically, and it is also
where the runnable transcript and the vector replay live.

## GET /meta

`GET /meta` answers a JSON body with the server string (`selvaged/<version>`, the same one
`--version` prints), the wire versions it speaks (both `selvage/1` and `selvage/2`, or
`selvage/1` alone with `--serve-version-1-only`), the roles it seats (`host`, `guest`), its
capabilities, and the keepalive and room-grace values it is configured with.

## Serving the page

`selvaged --serve-page DIR` serves the browser page from the same origin as `/session` and
`/meta`. One process, one port, one origin: the page's `/meta` read is same-origin, so it
needs no CORS proxy, and its socket is `ws://` or `wss://` on the page's own host, so there
is no cross-origin dial. The guest link is then a page link,
`http://HOST:PORT/?room=<room>&token=<token>`, with no `server=` parameter.

The page is the browser client's built `dist/`, which lives in the `web_client` repository;
the image builds it from a pinned revision and serves it.

`web_client` also publishes the bundle as a page-only image,
`ghcr.io/selvage-protocol/selvage-web`, whose own README owns the build, the tags and the
runtime. It is for putting the editor on its own origin, or for one page in front of several
servers. That second origin works because the page's socket is not CORS-bound and its
`/meta` read is only advisory, but it costs a second port and a second thing to upgrade, and
a link at that origin cannot reach a room's own page: the page reads the server from the
link's own address and from nowhere else, so an invite handed to a guest there is the wire
shape (`ws://HOST:PORT/session?room=…&token=…`), which fronting servers that serve no page
of their own hand out anyway. One origin stays the default: this image, whose page, `/meta`
and `/session` share one listener, and `--serve-page` from any deployment.

Served files carry the policy a browser needs: a media type from a pinned table,
`Cache-Control: no-cache` for a stable name and `public, max-age=31536000, immutable` for a
content-hashed one, `X-Content-Type-Options: nosniff`, a `Content-Security-Policy`, and
`Referrer-Policy: no-referrer`, because an invite URL carries the room token and must not
travel on in a `Referer` header.

`scripts/container-smoke.sh` builds the image with Docker, runs it read-only with every
capability dropped, asserts the page the image bakes, and joins a room in it with the
harness's client engine; `scripts/ci-local.sh container` runs the same where a Docker daemon
exists.

## The vectors

The wire protocol is described by the specification repository,
[`selvage-protocol/specification`](https://github.com/selvage-protocol/specification):
`PROTOCOL.md` is the prose, `CANONICAL.md` fixes the bytes of a frame, and `schema/` is the
machine-readable model. This repository authors none of it.

Its wire vectors are vendored here as `vectors/`, next to the harness that replays them, so
that a plain `cargo test` and the Nix sandbox need no sibling checkout.
`scripts/sync-vectors.sh` copies them in from a specification checkout when they change, and
the specification remains the canonical source.

The replay reads `vectors/`, or `SELVAGE_VECTORS` when that is set. The Nix build cannot see
outside the Cargo workspace, so `flake.nix` hands the directory in explicitly.

## What this slice does not do

No persistence, no accounts, no file access, no read-only guests, no E2EE, no editor
integration. `PROTOCOL.md` §12 lists every decision the design record leaves open.

`crates/client` hosts and joins a version-2 room over a socket (`relay.rs`, over `peer.rs` and
`host.rs`), but no **editor adapter** drives one: the relay exposes the session's own
observables and no editor surface, and the bridge that turns one into the other is not written
here. `SyncEngine` is still the version-1 engine and stays where it is; a version-2 link is
joined with `RelaySession` instead. Awareness is applied and not published — and not read
back either — so a version-2 session shows no cursor and `select` is not something a driver
can use. A dropped socket ends its session: `§9.1`'s return is unwired, as it is in the shared
engine whose relay runs no resume either.

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
a commercial product or service that substitutes for it. `packaging/README.md` records what the owner accepted for this project's published
image, and where that acceptance stops. Each release converts to MIT on the second
anniversary of the date it was made available, irrevocably.

The harness links `selvaged`, so its own `MIT OR Apache-2.0` covers the crate while a
redistributed `selvage-harness` binary carries FSL code with it. That redistribution must
include the FSL terms or a link to them and retain the copyright notices; the harness's
licence does not replace its dependency's FSL terms.

The vendored vectors are `CC-BY-4.0`
([`selvage-protocol/specification`](https://github.com/selvage-protocol/specification)), which
this repository does not author and redistributes with that repository as the source of the
attribution.
