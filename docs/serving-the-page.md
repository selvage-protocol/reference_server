# Serving the page

The page is the browser client's built `dist/`, which lives in the `web_client`
repository and is published as its own image,
`ghcr.io/selvage-protocol/selvage-web`. That image is the one-origin story: it
serves the page, and it relays `/session` and `/meta` to the server it is
configured with, so the page's `/meta` read is same-origin and its socket is
`ws://` or `wss://` on the page's own host, with no cross-origin dial and no CORS
proxy. The guest link is then a page link,
`http://HOST:PORT/?room=<room>&token=<token>`, with no `server=` parameter. A
self-host run is that image beside this server, which is what `compose.yaml`
writes; `web_client`'s README owns the image, its configuration and its tags.

`selvaged --serve-page DIR` serves a page you supply from the same origin as
`/session` and `/meta`, which is one process, one port and one origin with no
second container: mount the directory and name it on the command line, as
[Build the image from this checkout](../README.md#build-the-image-from-this-checkout)
does. The published image carries no page of its own and its own command passes no
page flag, so supplying one is the whole of that route.

The container serves as UID `65532`. Mounted page files need read permission,
and their directories need read and search permission for that UID, through
ownership, group membership, or mode bits. Inaccessible files return `404`. With
no page directory the server still answers `/meta` and `/session`, and `/` is
`404`.

Served files carry the policy a browser needs: a media type from a pinned table,
`Cache-Control: no-cache` for a stable name and
`public, max-age=31536000, immutable` for a content-hashed one,
`X-Content-Type-Options: nosniff`, a `Content-Security-Policy`, and
`Referrer-Policy: no-referrer`, because an invite URL carries the room token and
must not travel on in a `Referer` header.

`scripts/container-smoke.sh` builds the image with Docker, runs it read-only
with every capability dropped, asserts the version it answers `/meta` and
`/session` with, and joins a room in it with the harness's client engine; it then
runs it again with a page directory mounted and `--serve-page` naming it.
[`scripts/ci-local.sh container`](../scripts/ci-local.sh) runs the same where a
Docker daemon exists.
