# selvage-reference-server

A Rust workspace proving the Selvage session protocol end to end: a memory-only
reference server, a client library with an editor-adapter seam, and a headless
two-client harness that gates it in CI.

The server is `crates/selvaged`, the Rust client is `crates/client` and the
harness is `crates/harness`. This repository is the reference implementation of
the Selvage session protocol.

It works today: `selvaged` seats peers in a room and relays sealed frames it
cannot read, and `crates/client` hosts and joins a room over a socket. The harness
replays the specification's wire vectors against a server it starts and drives the
client through that corpus's decision vectors; no editor adapter is written yet.

## Get it working

The published image is the server alone, and a server alone is enough for a client
to join a room; the page is a second container, and the pair is printed by the
landing page's Run card, which owns the reader-facing commands: the three steps it
prints name the repositories bare, which docker reads as `:latest` — the tag the
release publishes — and it prints no teardown, so stopping the pair is `docker stop`
or `docker rm -f`, a step it leaves to the reader.

```sh
docker rm -f selvaged-solo 2>/dev/null
docker run -d --rm --name selvaged-solo -p 127.0.0.1:8081:8080 \
  ghcr.io/selvage-protocol/selvaged:latest
```

That is re-runnable: the removal clears the name a previous run left, and `--rm`
takes the container with it when it stops. Stopping it later, and taking it with
its logs, is a command of its own:

```sh
docker rm -f selvaged-solo
```

The name and the host port above are this example's own, so it and the card's pair
can be up at once: the card's server runs as `selvaged` and its page publishes
`8080`, so stopping either pair leaves the other's container alone. A client reaches
this container at `ws://127.0.0.1:8081` — the host port the mapping above publishes.
[Deploying](docs/deploying.md) has the other addresses a client might be pointed at,
and why they do not answer. This container serves the session protocol and no page,
so a client is what joins it; for a room you can open in a browser, run the two
containers the Run card prints, at <https://selvage.dontblameme.dev/#try>. The page
image is `web_client`'s and its README owns that image's configuration and its tags.
Both GHCR packages are public, so a pull needs no account and no `docker login`.

### From a checkout

With Nix, `cargo` and `rustc` come from the flake, so run cargo through the dev
shell:

```sh
nix develop . -c cargo run -p selvaged -- --listen 127.0.0.1:8080
```

Without Nix, [install Rust via rustup](https://www.rust-lang.org/tools/install)
(any recent stable works), and the same `cargo run` needs no other setup. For a
binary to keep, build it release and run that:

```sh
nix develop . -c cargo build --release --locked -p selvaged
./target/release/selvaged --listen 127.0.0.1:8080
```

### Build the image from this checkout

The image is the server alone, on one port:

```sh
docker buildx build --load -t selvaged:local .
docker run --rm -p 127.0.0.1:8080:8080 selvaged:local
```

`docker compose up` builds the same `Dockerfile` through `compose.yaml`, which runs
it beside the published page image, both read-only with every capability dropped.
This is the route for an image you are changing, or one built from your own
checkout.

To serve a page from that container, mount a directory and name it with
`--serve-page`: the image carries none, and its own command passes no such flag.

```sh
docker run --rm -p 127.0.0.1:8080:8080 -v "$PWD/page:/page:ro" \
  selvaged:local --listen 0.0.0.0:8080 --serve-page /page
```

### Nix

The flake's default package is `selvaged`, so `nix run` builds the binary and
starts it with the same flags:

```sh
nix run . -- --listen 0.0.0.0:8080
```

## Commands

`selvaged` is one binary with no configuration file, and the flags below are the
whole of its surface:

```text
usage: selvaged [--listen ADDR] [--room-grace-ms MS] [--serve-page DIR]
                [--max-connections N] [--max-rooms N] [--max-peers-per-room N]
                [--outbound-queue-bytes N] [--max-envelope-bytes N]
                [--inbound-bytes-per-sec N] [--inbound-burst-bytes N]
```

[What it prints, and its flags](docs/running.md) has the startup line, every flag
with its default, and which of those bounds are per process and which are per
connection.

`--help` prints the usage line, a short description of the server, and every flag
with its default; `--version` prints `selvaged/<version>`. The harness transcript
and the checks are in [the checks worth running](docs/development.md).

## More

- [What it prints, and its flags](docs/running.md): the startup line and the flag defaults.
- [What happens at a limit](docs/limits-and-sizing.md): what a peer sees, and how to size a box.
- [The server's shape](docs/the-server.md): rooms in memory, the grace period and `/meta`.
- [The first room](docs/rooms.md): the invite link, and which clients host or join.
- [Deploying](docs/deploying.md): the addresses a client dials, the bind and TLS.
- [Serving the page](docs/serving-the-page.md): `--serve-page` and the served headers.
- [The vectors](docs/the-vectors.md): where the corpus comes from and how it is replayed.
- [The client library and the harness](docs/client-and-harness.md): what each crate is.
- [The checks worth running](docs/development.md): the harness, the tests and `ci-local.sh`.
- [What this slice does not do](docs/what-this-slice-does-not-do.md): the gaps in this release.
- [Licence](docs/licence.md): what each path is under, and what the image carries.

## Licence

The server is `FSL-1.1-MIT`, source-available and not open source; the workspace
default is `MIT OR Apache-2.0`, which covers `crates/protocol`, `crates/client` and
`crates/harness`, and `vectors/` is vendored from the specification repository under
`CC-BY-4.0`. [Licence](docs/licence.md) has the terms, the paths and what the
published image carries.
