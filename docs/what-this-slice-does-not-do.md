# What this slice does not do

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
