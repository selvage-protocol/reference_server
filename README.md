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

```sh
cargo test                 # protocol unit tests, the vector replay, convergence and lifecycle

cargo run -p selvage-harness   # the whole slice, printed step by step
cargo run -p selvaged -- --listen 127.0.0.1:8080
```

`selvaged` serves `ws://…/session` and `http://…/meta`. It keeps nothing on disk: rooms die
with the host, after a 30-second grace period.

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
