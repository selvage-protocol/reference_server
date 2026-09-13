# impl — Selvage reference slice

A Rust workspace proving the session protocol end to end: a memory-only reference server,
a client library with an editor-adapter seam, and a headless two-client harness that gates
it in CI.

The wire protocol is described in [`../spec/PROTOCOL.md`](../spec/PROTOCOL.md); the design
record is [`../DESIGN.md`](../DESIGN.md).

## Layout

| crate | what it is |
|---|---|
| `crates/protocol` | `selvage/1` session envelope, method/event/error vocabulary, invite URLs. No I/O. |
| `crates/selvaged` | the server: rooms, membership, the open-document set, payload-opaque relay, `GET /meta` |
| `crates/client` | the sync engine (one `Y.Doc` per session, one `Y.Text` per document, y-protocols, awareness) and the `EditorAdapter` seam |
| `crates/harness` | one server plus N clients, driven programmatically; also a runnable transcript |

## Running it

```sh
cargo test                 # protocol unit tests, the convergence gate, lifecycle tests

cargo run -p selvage-harness   # the whole slice, printed step by step
cargo run -p selvaged -- --listen 127.0.0.1:8080
```

`selvaged` serves `ws://…/session` and `http://…/meta`. It keeps nothing on disk: rooms die
with the host, after a 30-second grace period.

## What this slice does not do

No persistence, no accounts, no file access, no read-only guests, no E2EE, no editor
integration. `spec/PROTOCOL.md` §12 lists every decision the design record leaves open.
