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

The protocol is `selvage/2`, the only version: a frame naming another is
`bad_message` (`PROTOCOL.md` §10). The server records membership and holds no
host, no open-document set, no grant and no document of any kind; its method
surface is `session.hello` and `session.rename`, and a `doc.*` request is
`unknown_method` with the connection left open. A binary frame is a sealed frame
the server relays byte for byte and cannot read, and no frame it authors carries
a path, a role or a document name. The room's life is its last connection: the
grace timer arms when the room's last connection ends, and the destruction has
no recipient, so it is silent and the next connection that names the id is told
`room_unknown`.

The room's keys, the sealed frame and the peers' own rules are `PROTOCOL.md`
§5.1, §7.1 and §13; this server's part of it is only the relay and the
membership.

## GET /meta

`GET /meta` answers a JSON body with the server string (`selvaged/<version>`,
the same one `--version` prints), the wire versions it speaks (`selvage/2`), its
capabilities, and the keepalive and room-grace values it is configured with.
`PROTOCOL.md` §2 has the shape.
