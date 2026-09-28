# The first room

The server mints nothing to share. It holds rooms in memory and waits for a
connection; the client that hosts mints the room and prints the invite link. The
invite is the permission, and the whole of it:

```text
ws://HOST:PORT/session?room=<room>&token=<token>#k=<room-key>&h=<host-key>
```

`room` and `token` are the server's to check; `k` and `h` are `PROTOCOL.md`
§5.1's room and host keys, which live in the fragment because a user agent never
sends one. A client that joins reads both, so a link without them is refused
before a socket is opened.

Anyone holding that link can join until the room dies. Host from an editor with
[`vscode_client`](https://github.com/selvage-protocol/vscode_client) or
[`nvim_client`](https://github.com/selvage-protocol/nvim_client); the
[`web_client`](https://github.com/selvage-protocol/web_client) page joins one as
a guest. A client connected to your server is what mints a room for a friend,
not the harness transcript.
