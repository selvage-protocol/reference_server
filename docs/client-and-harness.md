# The client library and the harness

`crates/client` is the `selvage/2` session: `sealed.rs` is `CANONICAL.md` §6.1's
bytes, `peer.rs` is `PROTOCOL.md` §13's decisions on top of them, `host.rs` is
§7.1's producer half and `relay.rs` puts a session on a socket. It reads no
`/meta` before dialling: a link carries everything a join needs.
`crates/harness` puts one server beside the tests that drive it, and it is also
where the runnable transcript and the vector replay live.
