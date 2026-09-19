# The Pi demo's shape

The exact files that run the live demo on `lumi-raspberrypi`, tracked here so a
deployment artefact has a reviewable home instead of existing only on the
machine. This is **the Pi demo's shape, not the container path**: the image in
`packaging/` runs one process on one port and needs no front, because a
container's port mapping is the boundary. Here the boundary is the tailnet and
the front is a TLS terminator outside the server.

The live state — the URL, the units, the boundaries, the upgrade procedure — is
`ai_notes/docs/runbook-pi-demo.md` (the project's private notes, not a path a
reader of this repository can open). That document owns those facts; this
directory owns the files, and the two must agree.

| File | Installs to | What it is |
|---|---|---|
| `selvaged.service` | `~/.config/systemd/user/selvaged.service` | The demo's user unit: the tailnet bind, the grace period, and `--serve-page` |
| `tls-proxy.py` | `~/selvage-demo/tls-proxy.py` | The TLS front: one certificate, every path relayed |
| `tls-proxy.service` | `~/.config/systemd/user/selvage-tls-proxy.service` | The front's user unit |
| `tls-proxy.env.example` | — | The front's environment, documented rather than installed |
| `test_tls_proxy.py` | — | The front's idle logic against fake sockets, run by the `tls-proxy` flake check — no Pi, no certificate |

## The shape

One TLS origin serves everything:

```text
https://lumi-raspberrypi.muskellunge-yo.ts.net:8444/         the page
https://lumi-raspberrypi.muskellunge-yo.ts.net:8444/meta     the meta document
wss://lumi-raspberrypi.muskellunge-yo.ts.net:8444/session    the room socket
                                    |
                          tls-proxy.py  (:8444, TLS)
                                    |
                     selvaged  (100.64.0.3:8080, plaintext)
                                    |
              the page directory  /home/pi/selvage-web/dist
```

`selvaged --serve-page DIR` serves the page on its own listener, so the page,
the meta read and the socket are the same origin. That removes the two defects
the previous shape worked around: the page's `/meta` fetch is no longer
cross-origin (no CORS headers anywhere), and its WebSocket dial is no longer a
plaintext request from an `https` page (no mixed-content refusal). The front
therefore has nothing to route, rewrite or answer — it terminates TLS with the
tailnet certificate and relays every path, byte for byte, to the address the
server binds.

Two consequences worth keeping:

- **The front reads no request.** No routing table, no header rewriting, no
  preflight. Anything added there is a second HTTP implementation to keep
  correct, which is what this replaced.
- **The backend address must name the tailnet bind explicitly.** Never
  loopback: on a shared host another user's server can already hold the
  loopback port, and every guest then lands on the wrong server while `/meta`
  answers identically on both.

## Boundaries

- **Tailnet only.** The front listens on the tailnet address, never `0.0.0.0`,
  and the certificate is the Tailscale-provisioned MagicDNS one. The invite
  link is the permission; there is no per-join approval in v1.
- **The page is not in this repository.** `~/selvage-web/dist/` is a copy of
  `web_client`'s built `dist/`, synced with `rsync -rli --checksum --delete`
  and proved by hashing the served bytes against both copies. The server reads
  the directory per request, so a re-sync needs no restart.
- **The binary is built on the Pi** from a `git archive` export of
  `reference_server` `main` (aarch64, the machine's own toolchain, `--locked`,
  `CARGO_BUILD_JOBS=2`). Nothing is installed system-wide and there is no
  cross-compile.
- **No Docker.** The image and compose file beside this directory are for
  other people's machines.

## Deploy and upgrade

The runbook's upgrade procedure is the one to follow; the short form, from a
checkout of `reference_server` at the new commit:

```sh
git archive <sha> | ssh pi@lumi-raspberrypi 'tar -x -C ~/selvage-demo/source'
ssh pi@lumi-raspberrypi 'cd ~/selvage-demo/source && echo <sha> > BUILD_SHA &&
  export PATH="$HOME/.cargo/bin:$PATH" && CARGO_BUILD_JOBS=2 cargo build --release --locked -p selvaged &&
  cd .. && cp -p selvaged selvaged.prev && systemctl --user stop selvaged &&
  cp source/target/release/selvaged selvaged && systemctl --user start selvaged'
```

`selvaged.prev` holds exactly one generation, so it is the rollback for the
binary. For the front, keep the previous file beside it (`tls-proxy.py.<why>`)
before overwriting; the file has no other history on the machine.

The units are copied the same way and need `systemctl --user daemon-reload`
plus a restart. The Pi's copies must hash-equal the files here:

```sh
ssh pi@lumi-raspberrypi 'sha256sum ~/selvage-demo/tls-proxy.py ~/.config/systemd/user/selvaged.service ~/.config/systemd/user/selvage-tls-proxy.service'
sha256sum tls-proxy.py selvaged.service tls-proxy.service
```

## Rollback

Every file this shape replaced is still on the Pi:

- `~/selvage-demo/tls-proxy.py.before-one-origin` — the routing front that
  served `/session` and `/meta` only, with the CORS headers.
- `~/selvage-demo/selvaged.service.before-one-origin` — the unit
  without `--serve-page`.
- `~/selvage-demo/selvaged.prev` — the binary before the page-serving
  build.
- `~/selvage-demo/tls-proxy.service.before-one-origin` — the front's unit
  under its old description.
- `~/selvage-web/serve.py` — the retired static page server, with
  `~/.config/systemd/user/selvage-web.service` stopped and disabled rather than
  removed.
- `~/selvage-web/dist.before-one-origin/` — the page directory as it stood
  before the mobile-pass re-sync.

Restoring the old shape is: copy each file back, `daemon-reload`,
`systemctl --user enable --now selvage-web`, restart the other two. Nothing
needs rebuilding.

## Certificate renewal

The front reads the certificate files at startup, so after the tailnet
certificate renews, `systemctl --user restart selvage-tls-proxy` and check the
log for the startup line. The certificate is read by path from where
`tailscale cert` wrote it — never copied into this directory, where it would go
stale.
