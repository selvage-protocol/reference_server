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
