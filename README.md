# selvage-reference-server

A Rust workspace proving the Selvage session protocol end to end: a memory-only
reference server, a client library with an editor-adapter seam, and a headless
two-client harness that gates it in CI.

## Get it working

`selvaged` is one binary with no configuration file, and the flags below are the
whole of its surface. The published image is the server alone, and a server
alone is enough for a client to join a room; the page is a second container, and
the pair is printed by the landing page's Run card, which owns the
reader-facing command with the tags inside it and the teardown under it.

```sh
docker rm -f selvaged-solo 2>/dev/null
docker run -d --rm --name selvaged-solo -p 127.0.0.1:8081:8080 \
  ghcr.io/selvage-protocol/selvaged:latest
```

That is re-runnable: the removal clears the name a previous run left, and
`--rm` takes the container with it when it stops. Stopping it later, and taking
it with its logs, is a command of its own:

```sh
docker rm -f selvaged-solo
```

The name and the host port above are this example's own, so it and the card's
pair can be up at once: the card's server runs as `selvaged` and its page
publishes `8080`, and neither its teardown nor this one touches the other's
container. A client reaches this container at `ws://127.0.0.1:8081` — the host
port the mapping above publishes. That is not the address the container's own
startup line prints: that line names the interface and port it bound inside the
container, `ws://0.0.0.0:8080/session`, and `0.0.0.0` is not an address to
dial. And it is not the `ws://127.0.0.1:8080` that `vscode_client`'s first
session names for a server on this machine: `8080` on this machine is the
card's page container while the pair is up, and that page relays the session
endpoint to the card's server, so an editor pointed at `8080` hosts on the
card's server rather than this one — and answers nothing when the pair is down.
This container serves the session protocol and no page, so a client is what
joins it; for a room you can open in a browser, run the two containers the Run
card prints, at <https://selvage.dontblameme.dev/#try>. The page image is
`web_client`'s and its README owns that image's configuration and its tags. Both
GHCR packages are public, so a pull needs no account and no `docker login`.

### From a checkout

With Nix, `cargo` and `rustc` come from the flake, so run cargo through the dev
shell:

```sh
nix develop . -c cargo run -p selvaged -- --listen 127.0.0.1:8080
```

Without Nix, [install Rust via rustup](https://www.rust-lang.org/tools/install)
(any recent stable works), and the same `cargo run` needs no other setup. For a
binary to keep, build it release and run that:

```sh
nix develop . -c cargo build --release --locked -p selvaged
./target/release/selvaged --listen 127.0.0.1:8080
```

### Build the image from this checkout

The image is the server alone, on one port:

```sh
docker buildx build --load -t selvaged:local .
docker run --rm -p 127.0.0.1:8080:8080 selvaged:local
```

`docker compose up` builds the same `Dockerfile` through `compose.yaml`, which
runs it beside the published page image, both read-only with every capability
dropped. This is the route for an image you are changing, or one built from your
own checkout.

To serve a page from that container, mount a directory and name it with
`--serve-page`: the image carries none, and its own command passes no such flag.

```sh
docker run --rm -p 127.0.0.1:8080:8080 -v "$PWD/page:/page:ro" \
  selvaged:local --listen 0.0.0.0:8080 --serve-page /page
```

The container serves as UID `65532`. Mounted page files need read permission,
and their directories need read and search permission for that UID, through
ownership, group membership, or mode bits. Inaccessible files return `404`. With
no page directory the server still answers `/meta` and `/session`, and `/` is
`404`.

The public demo is a tracked shape with its own compose file and proxy in
`deploy/`, and the deploy script `.github/workflows/deploy-prod.yml` runs on the
box. The published image is built multi-arch and tagged by
`.github/workflows/image.yml`.

### Nix

The flake's default package is `selvaged`, so `nix run` builds the binary and
starts it with the same flags:

```sh
nix run . -- --listen 0.0.0.0:8080
```

### What it prints, and its flags

The startup line names the endpoint and the limits this process is enforcing:

```text
selvaged listening on ws://127.0.0.1:8080/session (meta at http://127.0.0.1:8080/meta)
limits: 1024 connections, 1024 rooms, 128 peers per room, 32 MiB outbound per connection, 5 MiB inbound text envelope, 2 MiB/s inbound with a 64 MiB burst
```

With the default bind it goes on to note that the address is loopback-only and
how to widen it, how long rooms live after their last connection ends, and that
a client mints the room. `--help` prints the usage line, a short description of
the server, and every flag with its default.

```text
usage: selvaged [--listen ADDR] [--room-grace-ms MS] [--serve-page DIR]
                [--max-connections N] [--max-rooms N] [--max-peers-per-room N]
                [--outbound-queue-bytes N] [--max-envelope-bytes N]
                [--inbound-bytes-per-sec N] [--inbound-burst-bytes N]
```

| Flag                        | What it does                                                                                                                                                                                                                                                                                           |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--listen ADDR`             | bind `ADDR` (default `127.0.0.1:8080`)                                                                                                                                                                                                                                                                 |
| `--room-grace-ms MS`        | how long a room survives its last connection ending, in milliseconds (default `30000`, printed as 30s)                                                                                                                                                                                                 |
| `--serve-page DIR`          | serve the browser page from `DIR` on the same origin as `/session` and `/meta`                                                                                                                                                                                                                         |
| `--max-connections N`       | connections held at once, counted past the request head (default `1024`)                                                                                                                                                                                                                               |
| `--max-rooms N`             | rooms held at once; past it a room is not minted (default `1024`)                                                                                                                                                                                                                                      |
| `--max-peers-per-room N`    | peers one room seats at once (default `128`)                                                                                                                                                                                                                                                           |
| `--outbound-queue-bytes N`  | payload bytes queued but unwritten for one connection before it is dropped as a peer that stopped reading (default `33554432`, 32 MiB; the command line refuses one that cannot hold one whole frame, which is the 8 MiB frame bound plus the 64 KiB of envelope headroom the floor counts, `8454144`) |
| `--max-envelope-bytes N`    | the largest inbound text envelope the server will parse, judged on the frame's length before `serde_json` sees it (default `5242880`, 5 MiB)                                                                                                                                                           |
| `--inbound-bytes-per-sec N` | bytes one connection may send a second, refilled continuously (default `2097152`, 2 MiB)                                                                                                                                                                                                               |
| `--inbound-burst-bytes N`   | how much of that rate one connection may spend at once (default `67108864`, 64 MiB)                                                                                                                                                                                                                    |
| `--help`, `-h`              | print the usage and the flags                                                                                                                                                                                                                                                                          |
| `--version`                 | print `selvaged/<version>`                                                                                                                                                                                                                                                                             |

Every default is the reference value. The capacity flags are bounds on what one
process holds in memory and only that process can enforce them; the envelope
bound and the inbound budget are what one connection may send, and they are
judged in-process because a front cannot see either one. _Sizing a box_, below,
says which of a deployment's bounds belong where.

### What happens at a limit

Every limit refuses deterministically, and what a peer sees depends on the
limit:

| Limit reached                                       | What the peer sees                                                                                                                                                                                                                           |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--max-connections`                                 | a plain HTTP request is answered `503` with `retry-after`; a WebSocket upgrade is answered and then closed `1013`                                                                                                                            |
| `--max-rooms`                                       | a refusal, `session.error` with code `x.server_full`, then close `4000`                                                                                                                                                                      |
| `--max-peers-per-room`                              | `x.room_full` on the join, then close `4000`                                                                                                                                                                                                 |
| `--max-envelope-bytes`                              | a seated connection gets `session.error` `bad_message` naming the bound and the frame's size; the connection stays open. Before the handshake the same refusal closes `4000`                                                                 |
| `--inbound-bytes-per-sec` / `--inbound-burst-bytes` | `session.error` `x.rate_limited` naming the budget, then close `1013`; the room is told `peer.left` and a reconnect starts with a fresh budget. Before the handshake the same refusal closes `4000`, since every fault before seating closes |
| `--outbound-queue-bytes`                            | the peer is disconnected as one that stopped reading, and the room is told `peer.left`                                                                                                                                                       |

The rate limit is charged per inbound frame — the handshake's frames included,
which is where a peer can send frames nobody answers — at its payload size or
one kilobyte, whichever is larger: a flood of one-byte frames costs a kilobyte
of budget each, because that is closer to what a frame costs the server than its
payload is. Frames the session never parses — relayed document and awareness
payloads — are charged too, since a relay is copied once per peer. A refusal is
written before the socket closes; a peer that is still writing when the server
closes it can lose that refusal to a reset its own writes bring, which is the
same drop `PROTOCOL.md` §2.1 describes for an over-bound frame.

### Sizing a box

A small host should size the process rather than trust the defaults, which are
the reference values and assume headroom. `max_connections` multiplies the
per-connection outbound queue, so `connections × outbound-queue-bytes` is the
outbound ceiling the process can reach (at the defaults, 32 GiB), and
`--max-connections` is what a small box lowers first. The inbound budget bounds
what one connection can spend of the CPU.

The queue is also the floor under every frame the server sends, and it is
refused at startup if it cannot hold one. The largest frame is a relayed payload
(the frame bound, 8 MiB), the floor counts the envelope headroom around it as
well, and what is counted is the frame's wire bytes: the two together are
`8454144`, which is the smallest value the command line accepts. A queue below
that does not bound memory, it breaks sessions — a handshake frame nobody can
queue seats nobody — so the command line refuses the combination and says which
flag to move.

For a 1 GiB box with something else running on it, these are a defensible set:

```sh
selvaged --listen 0.0.0.0:8080 \
  --max-connections 32 --max-rooms 64 --max-peers-per-room 8 \
  --outbound-queue-bytes 8454144
```

That is an outbound ceiling of 258 MiB, not a figure anything reaches in a
session: it is every one of 32 connections holding a full queue of unwritten
frames at once, which is what the queue's own cap ejects. The two inbound bounds
keep their defaults here: 5 MiB is well above the largest sealed frame a peer
sends, and 2 MiB/s is already far above what an editor does. Lowering
`--inbound-bytes-per-sec` below a few tens of kilobytes a second will exile a
peer for traffic it did not choose to send: a client publishes presence on a
timer, and each of those frames costs a kilobyte of budget.

`selvaged` does not implement an idle deadline, and `PROTOCOL.md` §2.1 forbids
closing a seated session for silence: a connection that answers its pings is
never closed for being quiet. A deployment that needs one puts it in front, and
the half of it that is in-process already is `head_timeout` (5 s): a connection
that does not finish sending its request head inside that is closed, which is
what reclaims a half-open socket before it is ever counted against
`--max-connections`.

### The first room

The server mints nothing to share. It holds rooms in memory and waits for a
connection; the client that hosts mints the room and prints the invite link. The
invite is the permission, and the whole of it:

```text
ws://HOST:PORT/session?room=<room>&token=<token>
```

Anyone holding that link can join until the room dies. Host from an editor with
[`vscode_client`](https://github.com/selvage-protocol/vscode_client) or
[`nvim_client`](https://github.com/selvage-protocol/nvim_client); the
[`web_client`](https://github.com/selvage-protocol/web_client) page joins one as
a guest. A client connected to your server is what mints a room for a friend,
not the harness transcript.

### Letting someone else in

The default binds loopback, so only your own machine reaches it. For a friend to
join, bind an address they can reach and hand them a URL that names your
machine:

```sh
nix develop . -c cargo run -p selvaged -- --listen 0.0.0.0:8080
```

A free tunnel that forwards to your port works for a first test. Beyond that you
want a machine with a public address (a small VPS, and the right firewall
rules).

Plain `ws://` and `http://` is plaintext: the invite token travels in the clear,
so it is for a tailnet, a VPN or loopback. The protocol's transport security is
the deployer's to supply; put a TLS terminator in front (`tailscale serve`,
caddy, or your edge) and hand out the `https://` or `wss://` URL. There is no
TLS inside `selvaged`, and none is claimed.

### The checks worth running

The harness is the quickest way to watch the protocol work:

```sh
nix develop . -c cargo run -p selvage-harness                  # the whole slice, printed step by step
nix develop . -c cargo test --all-targets                      # unit tests, every integration suite and the examples' own
```

`cargo run -p selvage-harness` runs a scripted demo transcript: it starts a
server, mints a room, prints the invite link, walks two clients through it, and
exits. `selvaged` waits for a client to connect instead, and a client's output
carries the invite link.

`scripts/ci-local.sh` runs the same commands as `.github/workflows/ci.yml` on
this machine, one flake check per step, and needs `nix`:

```sh
scripts/ci-local.sh all        # the whole gate, and what the `checks` job runs
scripts/ci-local.sh nightly    # coverage, the rest of cargo-deny and cargo-audit (slow)
scripts/ci-local.sh lint       # actionlint over the workflow files, on its own
scripts/ci-local.sh image      # the nix-built image smoke, no Docker needed
scripts/ci-local.sh container  # docker build, docker run and a room join (needs Docker)
```

`all` is what the `checks` job runs; `nightly` is opt-in because it is slow. The
`image` workflow's two buildx jobs have no step here, since this host has no
Docker, let alone buildx; a pull request's checks are where they run.

`checks`, `nightly` and `image` refuse to run when the working tree differs from
`HEAD`, because what they build is the tracked tree at its working-tree content
and CI checks out the committed ref: an untracked file — a new test, a new
vector — is invisible to the build, so its green run would be of a smaller suite
than CI's. Commit, or stash, before running them.

## What lives here

| Path              | What it is                                                                                                                                                                                                                                                  |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/protocol` | the `selvage/2` session envelope, method/event/error vocabulary, invite URLs. No I/O.                                                                                                                                                                       |
| `crates/selvaged` | the server: rooms, membership, payload-opaque relay, `GET /meta`                                                                                                                                                                                            |
| `crates/client`   | the `selvage/2` client: the sealed frame (`sealed.rs`), the peer session (`peer.rs`), the host's producer half (`host.rs`) and the relay that puts a session on a socket (`relay.rs`)                                                                       |
| `crates/harness`  | one server and the waits the integration tests share; the wire corpus's replay over `vectors/`, the runnable transcript (`cargo run -p selvage-harness`), the peer layer's two suites, and `selvage-subject`, the client the corpus's decision layer drives |
| `vectors/`        | the wire vectors and the peer corpus, vendored from the specification; `scripts/sync-vectors.sh` refreshes them                                                                                                                                             |

## The server's shape

`selvaged` serves `ws://HOST:PORT/session` and `http://HOST:PORT/meta` on one
listener. It keeps nothing on disk: the rooms are in memory, so keep the process
running, because Ctrl-C ends all of them and a restart ends every room. A room
outlives its **last** connection by the grace period (`--room-grace-ms`, 30
seconds by default); rejoining with the same invite link inside that window
keeps the room.

A room holds its membership — a `peer_id`, a display name and an awareness
client id per connection — and nothing else. It holds nothing about what those
peers say: no part of the server reads a document payload or an awareness
payload, and the relay between peers is opaque to both. The invite token is the
permission, and the room is removed after the grace period with nobody in it.
There are no accounts and no file access.

### One wire version

The protocol is `selvage/2`. The server records membership and holds no host, no
open-document set, no grant and no document of any kind; its method surface is
`session.hello` and `session.rename`, and a `doc.*` request is `unknown_method`
with the connection left open. A binary frame is a sealed frame the server
relays byte for byte and cannot read, and no frame it authors carries a path, a
role or a document name. The room's life is its last connection: the grace timer
arms when the room's last connection ends, and the destruction has no recipient,
so it is silent and the next connection that names the id is told
`room_unknown`.

The peer side — the sealed frame, the room state, the holds and the client's own
rules — is specified in `PROTOCOL.md` §7.1 and §13. This server's part of it is
only the relay and the membership.

The client's part of it is `crates/client/src/peer.rs`:
`crates/client/src/sealed.rs` is `CANONICAL.md` §6.1's bytes, and `peer.rs` is
`PROTOCOL.md` §13 on top of them — the session keypair and its announcement, the
order of operations at a join, what may be published before and after a state
commits the connection's key, attribution by the key that verified, the holds
and their lease, and the two windows that end a session. It holds no socket: a
frame goes in, the decisions come out, and every clock is a value the caller
passes in, which is what lets the corpus drive it.

Around it are the two halves it was written to be handed. `host.rs` is §7.1's
producer — the room state's listing, roles and `issued` series, and the
`HostStore` a host that means to keep hosting keeps its key and its series in —
and `relay.rs` is the connection: it opens the WebSocket, says `session.hello`
at `selvage/2`, seats the session from `room.created`/`room.joined`, hands every
binary frame to it and every frame it produced to the socket, and runs its
clocks on a timer of its own. §5.1's two invite forms are one reading:
`relay::RelaySession::join` hands the link to `peer::PeerInvite::parse`, which
reads the room and token from the query and `k` and `h` from the fragment, and
the socket URL it dials has the fragment stripped. A fragment that is present
but is not both keys is refused where the link is read, in this client's own
words, rather than dialled: without both, the session can neither read a frame
nor verify one. `crates/harness/tests/relay_selvaged.rs` is that pair against a
real `selvaged`.

Two suites hold that layer. `crates/harness/tests/peer_vectors.rs` replays the
corpus's nineteen **frame** vectors against the sealed layer, and
`crates/harness/tests/decisions.rs` drives its seven **decision** vectors
against the client through `selvage-subject`, the binary that speaks the
corpus's subject protocol
(`cargo run -p selvage-harness --bin selvage-subject`). Six of the seven are
about what a client did with a frame it was handed; the other is about the
decision a link carries before any frame at all — §5.1's half-copied fragment —
which the subject answers as a refusal in its own words, decided by the client
library's own rule rather than by a copy of it. The specification's own runner
can drive the same binary:

```
python3 runner/run_peer.py --subject <checkout>/reference_server/target/debug/selvage-subject
```

All seven decision vectors pass, and each goes red under the guard it declares
it catches: the same suite removes the one guard a vector names and shows the
vector fail, so a rule vector cannot pass by asserting nothing.

## The client library and the harness

`crates/client` is the `selvage/2` session: `sealed.rs` is `CANONICAL.md` §6.1's
bytes, `peer.rs` is `PROTOCOL.md` §13's decisions on top of them, `host.rs` is
§7.1's producer half and `relay.rs` puts a session on a socket. It reads no
`/meta` before dialling: a link carries everything a join needs.
`crates/harness` puts one server beside the tests that drive it, and it is also
where the runnable transcript and the vector replay live.

## GET /meta

`GET /meta` answers a JSON body with the server string (`selvaged/<version>`,
the same one `--version` prints), the wire versions it speaks (`selvage/2`), its
capabilities, and the keepalive and room-grace values it is configured with.

## Serving the page

The page is the browser client's built `dist/`, which lives in the `web_client`
repository and is published as its own image,
`ghcr.io/selvage-protocol/selvage-web`. That image is the one-origin story: it
serves the page, and it relays `/session` and `/meta` to the server it is
configured with, so the page's `/meta` read is same-origin and its socket is
`ws://` or `wss://` on the page's own host, with no cross-origin dial and no CORS
proxy. The guest link is then a page link,
`http://HOST:PORT/?room=<room>&token=<token>`, with no `server=` parameter. A
self-host run is that image beside this server, which is what `compose.yaml`
writes; `web_client`'s README owns the image, its configuration and its tags.

`selvaged --serve-page DIR` serves a page you supply from the same origin as
`/session` and `/meta`, which is one process, one port and one origin with no
second container: mount the directory and name it on the command line, as the
section above does. The published image carries no page of its own and its own
command passes no page flag, so supplying one is the whole of that route.

Served files carry the policy a browser needs: a media type from a pinned table,
`Cache-Control: no-cache` for a stable name and
`public, max-age=31536000, immutable` for a content-hashed one,
`X-Content-Type-Options: nosniff`, a `Content-Security-Policy`, and
`Referrer-Policy: no-referrer`, because an invite URL carries the room token and
must not travel on in a `Referer` header.

`scripts/container-smoke.sh` builds the image with Docker, runs it read-only
with every capability dropped, asserts the version it answers `/meta` and
`/session` with, and joins a room in it with the harness's client engine; it then
runs it again with a page directory mounted and `--serve-page` naming it.
`scripts/ci-local.sh container` runs the same where a Docker daemon exists.

## The vectors

The wire protocol is described by the specification repository,
[`selvage-protocol/specification`](https://github.com/selvage-protocol/specification):
`PROTOCOL.md` is the prose, `CANONICAL.md` fixes the bytes of a frame, and
`schema/` is the machine-readable model. This repository authors none of it.

Its wire vectors are vendored here as `vectors/`, next to the harness that
replays them, so that a plain `cargo test` and the Nix sandbox need no sibling
checkout. `scripts/sync-vectors.sh` copies them in from a specification checkout
when they change, and the specification remains the canonical source.

The replay reads `vectors/`, or `SELVAGE_VECTORS` when that is set. The Nix
build cannot see outside the Cargo workspace, so `flake.nix` hands the directory
in explicitly.

`crates/harness/tests/vectors.rs` is that replay: each of the corpus's 24 wire
transcripts is driven against a server the harness starts, and every frame it
answers with is compared to the bytes the vector writes. None of the 24 is
skipped: a vector this server cannot answer is a defect to fix in the corpus or
in the server, never a hole in the sweep. The specification's own runner
(`runner/run_vectors.py`, against a built `selvaged`) replays the same files.

## What this slice does not do

No persistence, no accounts, no file access, no read-only guests, no E2EE, no
editor integration. `PROTOCOL.md` §12 lists every decision the design record
leaves open.

`crates/client` hosts and joins a room over a socket (`relay.rs`, over `peer.rs`
and `host.rs`), but no **editor adapter** drives one: the relay exposes the
session's own observables and no editor surface, and the bridge that turns one
into the other is not written here. Awareness is applied and not published — and
not read back either — so a session shows no cursor, `select` is not something a
driver can use, and the hello advertises `y-protocols/1` alone: §10 defines
`awareness` as a statement that the peer publishes presence, so a client that
advertises it while publishing none has told its peers to wait on cursors that
never come. A dropped socket ends its session: `§9.1`'s return is unwired.

## Licence

The server is licensed differently from everything beside it.

| Path                                                 | Licence                                                                                                         |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `crates/protocol`, `crates/client`, `crates/harness` | `MIT OR Apache-2.0`, the workspace default: [`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE) |
| `crates/selvaged`                                    | `FSL-1.1-MIT`: [source-available, _not_ open source](crates/selvaged/LICENSE)                                   |
| `vectors/`                                           | vendored from the specification repository, whose material is `CC-BY-4.0`                                       |

`crates/selvaged/Cargo.toml` carries `publish = false`, and
`cargo deny check licenses` is told about `FSL-1.1-MIT` for that one crate. The
Functional Source License 1.1 is free for any non-competing purpose: a company
self-hosting it internally is free, as are non-commercial education and
research. It forbids making the software available to others in a commercial
product or service that substitutes for it. The acceptance that governs this
project's published image is stated where the image is built:
`crates/selvaged/LICENSE` travels inside it, the
`org.opencontainers.image.licenses` label names the licence, and `Dockerfile`
and `.github/workflows/image.yml` record what was accepted and when. Each
release converts to MIT on the second anniversary of the date it was made
available, irrevocably.

The harness links `selvaged`, so its own `MIT OR Apache-2.0` covers the crate
while a redistributed `selvage-harness` binary carries FSL code with it. That
redistribution must include the FSL terms or a link to them and retain the
copyright notices; the harness's licence does not replace its dependency's FSL
terms.

The vendored vectors are `CC-BY-4.0`
([`selvage-protocol/specification`](https://github.com/selvage-protocol/specification)),
which this repository does not author and redistributes with that repository as
the source of the attribution.
