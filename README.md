# selvage-reference-server — the Selvage reference slice

A Rust workspace proving the session protocol end to end: a memory-only reference server,
a client library with an editor-adapter seam, and a headless two-client harness that gates
it in CI.

The wire protocol is described by the specification repository,
[`selvage-protocol/specification`](https://github.com/selvage-protocol/specification):
`PROTOCOL.md` is the prose, `CANONICAL.md` fixes the bytes of a frame, and `schema/` is the
machine-readable model. That repository is the canonical source for all three, and this one
authors none of them.

The wire vectors are **vendored** here as `vectors/`, next to the harness that replays them,
so that a plain `cargo test` and the Nix sandbox need no sibling checkout.
`scripts/sync-vectors.sh` copies them in from a specification checkout when they change; the
specification remains the canonical source.

## Layout

| crate | what it is |
|---|---|
| `crates/protocol` | `selvage/1` session envelope, method/event/error vocabulary, invite URLs. No I/O. |
| `crates/selvaged` | the server: rooms, membership, the open-document set, payload-opaque relay, `GET /meta` |
| `crates/client` | the sync engine (one `Y.Doc` per session, one `Y.Text` per document, y-protocols, awareness) and the `EditorAdapter` seam |
| `crates/harness` | one server plus N clients, driven programmatically; also a runnable transcript, and the vector replay over `vectors/` |
| `vectors/` | the wire vectors, vendored from the specification; `scripts/sync-vectors.sh` refreshes them |

## Running it

You need a Rust toolchain: [install Rust via rustup](https://www.rust-lang.org/tools/install)
(any recent stable works). With Nix, `nix develop` provides the pinned toolchain instead —
no other setup either way.

```sh
cargo test                 # protocol unit tests, the vector replay, convergence and lifecycle

cargo run -p selvage-harness   # the whole slice, printed step by step
cargo run -p selvaged -- --listen 127.0.0.1:8080
```

The two commands do different things. The harness runs a scripted demo transcript: it
starts a server, mints a room, prints the invite link, walks two clients through it, and
exits. `selvaged` is the server alone: it waits for a client to connect, and a client's
output carries the invite link — the server itself mints nothing to share. To host a room
for a friend you need a client connected to your server, not the harness transcript.

`selvaged` serves `ws://HOST:PORT/session` and `http://HOST:PORT/meta`. It keeps nothing
on disk: keep this process running — Ctrl-C ends all rooms. Rooms outlive a lost
connection by a 30-second grace period (`--room-grace-ms`); keep your invite link, since
rejoining with it inside the window reclaims the room.

The loopback default above reaches only your own machine. For a friend to join, bind an
address they can reach and hand them a URL that names *your* machine, not localhost:

```sh
cargo run -p selvaged -- --listen 0.0.0.0:8080
```

A free tunnel that forwards to your port works for a first test; beyond that you want a
machine with a public address (a small VPS, the right firewall rules). To run the server
beyond a shell — a systemd user unit, or a multi-arch container image with a
one-service compose file — see `packaging/` (install docs, upgrade flow, version
truthfulness, and the FSL-1.1-MIT redistribution the published image carries).

### One container, one port, and the page

`selvaged --serve-page DIR` serves the browser page from the same origin as `/session`
and `/meta`. One process, one port, one origin: the page's `/meta` read is same-origin
(no CORS proxy) and its socket is `ws://` or `wss://` on the page's own host (no
cross-origin dial). The guest link is then a page link —
`http://HOST:PORT/?room=<room>&token=<token>` — with no `server=` parameter.

The page itself is the browser client's built `dist/`, which lives in the `web_client`
repository. The container image builds it from a pinned revision and serves it, so a
container needs no mount:

```sh
docker run --rm -p 127.0.0.1:8080:8080 ghcr.io/selvage-protocol/selvaged
```

A page built elsewhere overrides the baked one by mounting over it — the image's own
command already passes `--serve-page /page`:

```sh
docker run --rm -p 127.0.0.1:8080:8080 \
  -v "$PWD/page:/page:ro" \
  ghcr.io/selvage-protocol/selvaged
```

With no page directory the server still answers `/meta` and `/session`; `/` is `404`.
`compose.yaml` runs the image hardened and documents the same override.

Served files carry the policy a browser needs: a media type from a pinned table
(never the host's mime database), `Cache-Control: no-cache` for a stable name and
`public, max-age=31536000, immutable` for a content-hashed one, `Referrer-Policy:
no-referrer` — an invite URL carries the room token, which must not travel on in a
`Referer` header — `X-Content-Type-Options: nosniff`, and a `Content-Security-Policy`.
`scripts/container-smoke.sh` builds the image with Docker, runs it read-only with every
capability dropped, asserts the page the image bakes, and joins a room in it with the
harness's client engine; `scripts/ci-local.sh container` runs the same where a Docker
daemon exists.

**Not secure by default.** `ws://`/`http://` is plaintext: the invite token travels in
the clear, so it is for a tailnet, a VPN or loopback. The protocol's transport security
is the deployer's to supply; put a TLS terminator in front — `tailscale serve`, caddy,
or your edge — and hand out the `https://`/`wss://` URL. There is no TLS inside
`selvaged`, and none is claimed. Rooms are memory-only: a restart ends every room.

The vector replay reads `vectors/`, or `SELVAGE_VECTORS` when that is set — the Nix build
cannot see outside the Cargo workspace, so `flake.nix` hands the directory in explicitly.

## What this slice does not do

No persistence, no accounts, no file access, no read-only guests, no E2EE, no editor
integration. `PROTOCOL.md` §12 lists every decision the design record leaves open.

## Licence

**The server is licensed differently from everything beside it.**

| Path | Licence |
|---|---|
| `crates/protocol`, `crates/client`, `crates/harness` | `MIT OR Apache-2.0`, the workspace default — [`LICENSE-MIT`](LICENSE-MIT) and [`LICENSE-APACHE`](LICENSE-APACHE) |
| `crates/selvaged` | **`FSL-1.1-MIT`** — [source-available, *not* open source](crates/selvaged/LICENSE) |
| `vectors/` | vendored from the specification repository, whose material is `CC-BY-4.0` |

`crates/selvaged/Cargo.toml` carries `publish = false`, and `cargo deny check licenses` is
told about `FSL-1.1-MIT` for that one crate. The Functional Source License 1.1 is free for
any non-competing purpose — a company self-hosting it internally is free, as are
non-commercial education and research — and forbids making the software available to others
in a **commercial** product or service that substitutes for it. A free competing relay is not
a Competing Use under that text. Each release converts to MIT on the second anniversary of
the date it was made available, irrevocably.

The harness links `selvaged`, so its own `MIT OR Apache-2.0` covers the crate while a
**redistributed** `selvage-harness` binary carries FSL code with it.

The vendored vectors are `CC-BY-4.0` ([`selvage-protocol/specification`](https://github.com/selvage-protocol/specification)),
which this repository does not author and redistributes with that repository as the source of
the attribution.
