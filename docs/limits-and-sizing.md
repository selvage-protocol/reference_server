# What happens at a limit

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

## Sizing a box

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
keep their defaults here. `--max-envelope-bytes` bounds the text envelope alone,
judged on the frame before the parser sees it, and at 5 MiB it sits below the
8 MiB transport bound so that an over-bound envelope is refused as one rather
than by ending the connection; a sealed frame is a binary frame, and the
transport bound is what holds that. And 2 MiB/s is already far above what an
editor does. Lowering `--inbound-bytes-per-sec` below a few tens of kilobytes a
second will exile a peer for traffic it did not choose to send: a client
publishes presence on a timer, and each of those frames costs a kilobyte of
budget.

`selvaged` does not implement an idle deadline, and `PROTOCOL.md` §2.1 forbids
closing a seated session for silence: a connection that answers its pings is
never closed for being quiet. A deployment that needs one puts it in front, and
the half of it that is in-process already is `head_timeout` (5 s): a connection
that does not finish sending its request head inside that is closed, which is
what reclaims a half-open socket before it is ever counted against
`--max-connections`.
