# The server's shape

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

## One wire version

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

## GET /meta

`GET /meta` answers a JSON body with the server string (`selvaged/<version>`,
the same one `--version` prints), the wire versions it speaks (`selvage/2`), its
capabilities, and the keepalive and room-grace values it is configured with.
