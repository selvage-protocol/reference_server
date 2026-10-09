# The first room

The server mints nothing to share. It holds rooms in memory and waits for a
connection; the client that hosts mints the room and prints the invite link. The
invite is the permission to join, and the whole of it: the room and token in the
URL's query, and the room and host keys in its fragment, which a user agent
never sends. `PROTOCOL.md` §5.1 has the shape and both forms, and a link a
client joins by needs all four; one without them is refused before a socket is
opened.

Anyone holding that link can join until the room dies. Host from an editor with
[`vscode_client`](https://github.com/selvage-protocol/vscode_client) or
[`nvim_client`](https://github.com/selvage-protocol/nvim_client); the
[`web_client`](https://github.com/selvage-protocol/web_client) page joins one as
a guest. A client connected to your server is what mints a room for a friend.
