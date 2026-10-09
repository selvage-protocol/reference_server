# Deploying

## The addresses a client dials

The container's own startup line is not an address to dial: it names the interface
and port the process bound inside the container, `ws://0.0.0.0:8080/session`, and
`0.0.0.0` is not an address to dial. Nor is it the `ws://127.0.0.1:8080` that
`vscode_client`'s first session names for a server on this machine: `8080` on this
machine is the card's page container while the pair is up, and that page relays the
session endpoint to the card's server, so an editor pointed at `8080` hosts on the
card's server rather than this one — and answers nothing when the pair is down.
This container serves the session protocol and no page, so a client is what
joins it; for a room you can open in a browser, run the two containers the Run
card prints, at <https://selvage.dontblameme.dev/#try>. The page image is
`web_client`'s and its README owns that image's configuration and its tags.

## Letting someone else in

The default binds loopback, so only your own machine reaches it. For a friend to
join, bind an address they can reach and hand them a URL that names your
machine:

```sh
nix develop . -c cargo run -p selvaged -- --listen 0.0.0.0:8080
```

A free tunnel that forwards to your port works for a first test. Beyond that you
want a machine with a public address (a small VPS, and the right firewall
rules).

Plain `ws://` and `http://` is plaintext: the invite token travels in the clear,
so it is for a tailnet, a VPN or loopback. The protocol's transport security is
the deployer's to supply; put a TLS terminator in front (`tailscale serve`,
caddy, or your edge) and hand out the `https://` or `wss://` URL. There is no
TLS inside `selvaged`, and none is claimed.

## The published image and the tracked shape

The image is built multi-arch, so an arm64 host pulls the same tag as an amd64
one, and it carries the server alone. The public demo is a tracked shape of its
own in [`deploy/`](../deploy): a compose file, a proxy and an update timer.
[`compose.yaml`](../compose.yaml) at the root is the self-host shape, and
`docker compose up` runs the server beside the published page image, both
read-only with every capability dropped.
