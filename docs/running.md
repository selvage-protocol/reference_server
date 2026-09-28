# What it prints, and its flags

The startup line names the endpoint and the limits this process is enforcing:

```text
selvaged listening on ws://127.0.0.1:8080/session (meta at http://127.0.0.1:8080/meta)
limits: 1024 connections, 1024 rooms, 128 peers per room, 32 MiB outbound per connection, 5 MiB inbound text envelope, 2 MiB/s inbound with a 64 MiB burst
```

With the default bind it goes on to note that the address is loopback-only and
how to widen it, how long rooms live after their last connection ends, and that a
client mints the room.

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
judged in-process because a front cannot see either one. [Sizing a box](limits-and-sizing.md)
says which of a deployment's bounds belong where.
