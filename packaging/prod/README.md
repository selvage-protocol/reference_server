# The public demo's shape

The exact files that run `selvage-demo.dontblameme.dev`, tracked here so the
deployment's shape is a reviewable artefact in a repository.

The origin is public and the owner's decision of 2026-09-21 was a page container
of its own, so this front terminates TLS *and* routes, and `selvaged` serves
`/session` and `/meta` alone.

The live state — the host, the images running, the network security group, the
upgrade procedure — is `ai_notes/docs/runbook-prod-demo.md` (the project's own
notes, not a path a reader of this repository can open). That document owns
those facts; this directory owns the files, and the two must agree.

| File | Installs to | What it is |
|---|---|---|
| `compose.yaml` | `/srv/selvage/compose.yaml` | The three services, the hardening and the memory limits |
| `.env.example` | `/srv/selvage/.env` | The two image tags the next `up` runs, and the timer follows |
| `selvage-update.service` | `/etc/systemd/system/` | One pull and `up -d` as `selvage`, then a prune |
| `selvage-update.timer` | `/etc/systemd/system/` | Runs that every five minutes |
| `deploy.py` | `/usr/local/sbin/selvage-deploy` | The CI deploy: reads one request on stdin, rewrites those `.env` lines, pulls and ups |
| `deployci.sudoers` | `/etc/sudoers.d/selvage-deploy` | The one root command the CI user on this box may run |
| `test_deploy.py` | nowhere; run where it is | What the request grammar refuses, that a failed deploy leaves `.env` alone, and that the units and the shape agree with the script |
| `proxy/Dockerfile` | build context `/srv/selvage/proxy/` | The front's image, from the two files below it |
| `proxy/nginx.conf` | baked into that image | Process and http scope: the log policy, the temp paths, the per-source zones |
| `proxy/conf.d/default.conf` | baked into that image | The server block: TLS, the routes, the limits, the terms banner, and the location that serves the page below |
| `proxy/conf.d/cloudflare-ips.conf` | baked into that image | Cloudflare's published ranges, for `real_ip` |
| `proxy/www/terms.html` | baked into that image at `/usr/share/selvage/www/` | The terms of this instance, which is the page `/terms` answers with |
| `check-terms.sh` | nowhere; run where it is | The notice read back out of the bytes the front serves, without Docker |
| `test_front_limits.py` | nowhere; run where it is | That the front serves one name, refuses a source, and that one metered endpoint cannot spend another's budget |

## The shape

One origin serves everything, one port is published, and the split is by path:

```text
              browser
                 |  https://selvage-demo.dontblameme.dev/...
                 v
          Cloudflare  (proxied DNS, Full (strict), redirect at the edge)
                 |  TCP 443, from Cloudflare's published ranges only
                 v
    +---------------------------------  Azure Standard_B2ats_v2
    |  proxy            (nginx, uid 101, port 443 -> 8080, TLS)
    |    /session  ------------------>  selvaged   :8080   WebSocket
    |    /meta     ------------------>  selvaged   :8080   JSON
    |    /terms    ------------------>  read from the front's own image
    |    everything else  ----------->  selvage-web :8080   the page
    +------------------------------------------------------------
```

Each arrow is a Docker network hop; none of the three publishes a port except
the front, and the certificate mount is the only mount in the deployment.

## The network security group is not in this repository

Azure's NSG, not this compose file, is what makes the origin unreachable except
through Cloudflare. Read the rules and change them with:

```sh
az network nsg rule list -g main --nsg-name selvage-protocol-prod-nsg -o table
az network nsg rule update -g main --nsg-name selvage-protocol-prod-nsg -n HTTPS --access Deny
```

The state it is in, set 2026-09-21:

| Priority | Rule | Effect |
|---|---|---|
| 300 | `HTTP` | **Deny** inbound TCP 80. Cloudflare redirects at its edge, and the origin has no listener there anyway. |
| 310 | `HTTPS-Cloudflare-v4` | Allow inbound TCP 443 from Cloudflare's 15 published IPv4 ranges |
| 311 | `HTTPS-Cloudflare-v6` | Allow inbound TCP 443 from Cloudflare's 7 published IPv6 ranges |
| 320 | `HTTPS` | **Deny** inbound TCP 443 from anywhere else |

The one fact to keep in step with `proxy/conf.d/cloudflare-ips.conf` is the
range list itself: refresh both together from
`https://api.cloudflare.com/client/v4/ips`, which is where both lists came from.
Port 22 is not in the NSG at all and is refused by the default rule; hands-on
access is Tailscale SSH over the tailnet, which the NSG does not touch.

## What each container is allowed

`docker compose config` parses the file and resolves its variables; the daemon's
record of the containers is what the readback is from, because "the file says
`read_only`" is a claim about a file and not about a container. The readback run
at bootstrap — the same flags, read back from `docker inspect` for all three
images, before the deployment was started by compose — is in
`ai_notes/.tmp/prod-deploy-prep-2026-09-21.md`.

```sh
docker inspect --format '{{.Name}} ro={{.HostConfig.ReadonlyRootfs}} caps={{.HostConfig.CapDrop}} nnp={{.HostConfig.SecurityOpt}} user={{.Config.User}} mounts={{range .Mounts}}{{.Source}} {{end}} mem={{.HostConfig.Memory}}' \
  selvage-prod-proxy-1 selvage-prod-selvaged-1 selvage-prod-selvage-web-1
```

Every one of the three: read-only root filesystem, `cap_drop: [ALL]`,
`no-new-privileges:true`, a non-root user (`101` for the two nginx images,
`65532` from the server image's own `USER`), no `SYS_ADMIN`, no Docker socket, no
`privileged`, and a `mem_limit`. The front has exactly two mounts and they are
the two halves of one certificate; the other two have none.

## The limits, and why these numbers

The box has 2 vCPU, 892 MiB of RAM and a 10 GiB swapfile. Three containers and
the Docker daemon have to fit in that, so the server is told what it may hold
rather than left on its reference defaults, which assume headroom this host does
not have (at the defaults, `--max-connections × --outbound-queue-bytes` is 32 GiB).

| Flag | Value | Why this one |
|---|---|---|
| `--max-connections` | 32 | Multiplies the per-connection queue, so it is the term that sizes the process: 32 × 8454144 is a 258 MiB outbound ceiling, the largest surprise this host can absorb. A demo may have 32 live sockets; the server's own README ("Sizing a box") offers 32 as a defensible set for a box around this size. |
| `--outbound-queue-bytes` | 8454144 | The floor, and a constant of the binary rather than of these flags: `ServerConfig::smallest_queue_bytes` is the 8 MiB frame bound (`net::MAX_FRAME_BYTES`, the largest frame a peer may relay) plus the 64 KiB of envelope headroom it counts for the peers list and the envelope around them, so 8388608 + 65536 = 8454144 is the smallest queue that can hold one whole frame. Below it `selvaged` exits 2 with the floor named, which on a `restart: unless-stopped` container is a restart loop, and the front behind it is down with it. Nothing above the floor is wanted here: 32 × it is the memory surprise this box has to absorb. |
| `--max-rooms` | 64 | Rooms held at once. More than a demo needs, and each one holds a token and up to eight peers — membership, and nothing larger. |
| `--max-peers-per-room` | 8 | One room is a session with a friend; eight peers is room for that and three who looked at the wrong link. |
| `--max-envelope-bytes` | 5242880 | The default, unchanged on purpose: a text envelope is a session frame and never a payload, so 5 MiB is already far above any frame the server parses. A frame past it is refused with the bound named and the connection stays open. |
| `--inbound-bytes-per-sec` | 1048576 | Half the default. The bound is on the JSON parse and the room relay, and a keystroke is tens of bytes billed at a 1 KiB floor, so a megabyte a second is still a hundred times what an editor sends. |
| `--inbound-burst-bytes` | 33554432 | Half the default. A fresh connection starts with the whole burst, so this is the shape a flood of new connections can spend before the rate bites; 32 MiB still clears any initial sync a room this size can have. |
| `--room-grace-ms` | 30000 | The reference value, deliberately: `/meta` advertises it and both clients size their reconnect budget from it. Changing it here alone would desync them. |

`mem_limit` is 48m, 48m and 320m, and the arithmetic is measured rather than
assumed: with all three containers up the box reports about **280 MiB
available**, so the backstop's question is only whether one container can cross
that line on its own. The host needs about 470 MiB to stay alive (892 total,
measured, minus what Docker and Ubuntu and Tailscale take), so a single
container's cap only has to sit under the remaining 420 MiB, and `320m` does.
The two nginx caps are far below anything they can reach. Past its cap a
container is killed inside its own cgroup and `restart: unless-stopped` brings
it back in seconds, having dropped every room, rather than the kernel picking a
victim on the host.

One term can reach the cap, and it is the one these numbers were chosen around.

| term | worst case as configured | bounded by |
|---|---|---|
| outbound queues | 32 × 8454144 = 258 MiB | `--max-connections`, `--outbound-queue-bytes` |

The outbound queue is where the memory is: 32 connections × 8454144 is 258 MiB if all of
them hold a full queue, which a peer that stops reading can arrange, and that is what
sizes the `320m` limit. The room state is membership — a token, a peer record and a queue
handle per connection, at most eight to a room — so 64 rooms cost kilobytes rather than
the megabytes a listing did. Nothing a visitor uploads is stored: a text envelope is
parsed and dropped, and a binary frame is relayed and dropped.

## What the front does that the server cannot

`PROTOCOL.md` §12 requires a public deployment to supply a connection cap, an
idle deadline and a rate limit in front of the server, and requires that a
deployment not log request URLs. The split is the one `ai_notes` settled when the
capacity flags landed:

- **One name, and only one.** The front's block is the only server on its port, so
  nginx makes it the default and would answer *any* name that resolves to this
  origin — `selvage.dontblameme.dev` is the project's landing page and is served
  somewhere else entirely. A request whose `Host` is not the name that block is
  the server for is closed with `444` — no response at all — rather than given the
  demo's page or its socket. The name is written once, in the block's
  `server_name`, and the guard compares the request against it; the NSG above
  means no such request arrives from outside Cloudflare today, which makes this
  depth rather than the only lock.

- **Per-source caps at the front.** `selvaged` has no per-source view at all.
  `limit_conn` allows one source 32 connections to the page, 8 concurrent
  `/session` sockets and 4 concurrent `/meta` reads; `limit_req` allows 5 `/session`
  handshakes a second with a burst of 10 and 5 `/meta` reads a second with the
  same burst. Each metered location names a zone of its own, for both
  directives, so one endpoint's count cannot spend another's — the page reads
  `/meta` on every load and opens `/session` on the click that follows, so a
  shared request zone is a visitor refused at the join. Past either limit the
  source gets a **429**. The keys are the end client's address: `real_ip` is
  told to trust `CF-Connecting-IP` from Cloudflare's ranges and from nowhere
  else, so a connection that did not come through Cloudflare cannot name its own
  source.

- **Total caps at the server.** `--max-connections` and the rest, above.

- **Idle deadline.** No in-process idle reaper exists and §2.1 forbids one for a
  seated session. The front's `proxy_read_timeout` is 300 s, an order of
  magnitude above the server's 30-second ping, so it only ever fires on a socket
  that is genuinely gone; Cloudflare's own 100-second idle close is above the
  ping too. **Nothing here lengthens the ping interval**: 30 s is what keeps a
  live session inside Cloudflare's 100 s.

- **No request URLs in a log.** The access log is `off`, and the error log is at
  `crit`, above every level at which nginx attaches the request line. That second
  half is deliberate and is not free: the two lines this deployment would
  otherwise have seen — a `limit_req`/`limit_conn` refusal, and a failed connect
  to an upstream that is being recreated — are the two that carry the URL, so
  the choice is between seeing them and never logging a token. The protocol's
  MUST wins. What is left is process-level faults and nothing request-scoped:
  `error_log` takes a file or `stderr` and no filter, so a level is the only
  control nginx offers. The residual is stated in `proxy/nginx.conf` beside the
  directive: a `crit`-level message emitted while a request is in flight would
  carry the request line. None was producible.

- **A dead upstream fails in seconds, not in a minute.** `proxy_connect_timeout`
  is 5 s on all three locations. Docker removes a stopped container's address
  from the network, so a connect to it is dropped rather than refused and
  nginx's 60-second default would hold the visitor's socket open saying nothing;
  the observed result is a `504` after 5.0 s.

**In normal operation the front logs nothing at all.** Its whole `docker logs`
is empty until something at `crit` happens. That is deliberate, and the things an
operator normally reads the front's log for are answered elsewhere — `docker compose ps` for whether it is up, the server's own startup line for the limits in
force, and the isolated proof below for whether a change broke routing.

The level is load-bearing and is tested: the isolated proof runs a second container
from the same image with only
`error_log` changed to `error` and the same traffic through it, and the token
appears **28 times** in that container's log. The `crit` front saw the same
requests and logged nothing.

**The front is not the only container that can see an invite link.** A guest's
link is a *page* link — `/?room=…&token=…` — so it lands on `selvage-web`, and
that image keeps an nginx access log on its stdout. The cutover found it doing
exactly that on the public URL:

    172.18.0.4 - - [21/Sep/2026:13:28:45 +0000] "GET /?room=r-LEAKTEST&token=<probe token> HTTP/1.1" 200 32281 "-" "curl/8.22.0"

The probe invented both the room and the token; the token is elided here because
`ripsecrets`, which this repository runs on every commit, reads a literal
`token=`-followed-by-a-value as one of its patterns, and it was right to.

`selvage-web` therefore runs with Docker's `none` logging driver: its output is
discarded outright. Discarding rather than filtering is the point — it holds
whatever the image's own configuration does, so bumping `SELVAGE_WEB_IMAGE`
cannot quietly bring the leak back, and the alternative (a derived page image
with the access log patched out) is a second copy of another repository's nginx
configuration to keep in step. The price is named in `compose.yaml` beside the
directive: this container's errors are discarded too. It is a static file server,
and the front and the server beside it show the failures that matter.

If `web_client` turns its own access log off — or shortens the format to `$uri`,
which carries no query string — the driver can come back and this becomes the
image's guarantee instead of the deployment's.

## Transport security is the edge's, and the front is silent about it

**Nothing in this directory sets `Strict-Transport-Security`**, and nothing here
has ever set it: no file under `packaging/prod/` carries an `add_header` at all.
What a visitor receives for this host is Cloudflare's, added at the zone:

```console
$ curl -sS -o /dev/null -D - https://selvage-demo.dontblameme.dev/ | grep -i strict-transport
strict-transport-security: max-age=0; includeSubDomains; preload
```

That field is the edge's and not the origin's, and the response that shows it
does not settle the question — both layers answer on this hostname. Two others
do. `https://selvage-demo.dontblameme.dev/cdn-cgi/trace` is answered by Cloudflare
and never reaches an origin, and it carries the same field; so does the zone's
apex, where `dontblameme.dev` answers a `301` to another site from the edge.
The value is Cloudflare's HSTS setting with a max-age of `0`, which is that
dashboard's own "Disable HSTS", with the include-subdomains and preload toggles
left on beside it.

What a browser does with it, read from `RFC 6797` §8.1 rather than assumed: a
max-age of `0` means the user agent *removes* a policy it has stored for the
host, or does not note the host at all, so the field enforces nothing; over
plain HTTP any HSTS field is ignored outright; and where two arrive in one
response the user agent processes only the first. The domain's reach does not
depend on it either way, because `.dev` is on the preload list —
`{"name": "dev", "policy": "public-suffix", "mode": "force-https", "include_subdomains": true}`,
verbatim from Chromium's `transport_security_state_static.json` — so a browser upgrades this host to HTTPS before it sends anything and a
`max-age=0` header cannot take that away. The field is a claim and not a
control — which is why an inert one is worth correcting rather than leaving to
look deliberate.

**Why the front must not set one.** A field added here would reach a visitor
only through Cloudflare, which already emits one for every response on the
zone — including the ones no origin writes, such as the apex redirect and the
edge's own endpoints, which is the half an origin can never cover. A policy is
a property of the host and not of the response that carried it, so a second
field would buy nothing and would put which policy a browser applies at the
mercy of header order. HSTS belongs to the zone, and this directory's
contribution to transport security is the one Cloudflare does not make: TLS
1.2 and 1.3 only on the origin listener, and an origin no one but Cloudflare
can reach (`proxy/conf.d/default.conf`, and the network security group whose
rules are above).

**What the owner can change, and what it costs.** The setting is one control
in the Cloudflare dashboard (SSL/TLS → Edge Certificates → HSTS) and no
credential for it exists in any repository, so this is not a deploy and not a
file. Turning HSTS off at the zone is the option that leaves no claim behind.
Setting a real max-age is not a one-line fix: `includeSubDomains` and
`preload` are already on beside it, and both apply to **the whole of
`dontblameme.dev`** — `includeSubDomains` would break any plain-HTTP service on
any subdomain of it for every browser that has seen the field, and a preload
entry is slow to remove and is served from a list the browsers ship. A demo
instance with no long-term commitment wants a short max-age if it wants one at
all, and neither of those two tokens; enabling them is a decision about the
domain rather than about this instance.

The live state of that control is `ai_notes/docs/runbook-prod-demo.md`, which
owns the host and the dashboard; this section owns the reason the front is
silent, and `test_front_limits.py` is where it is measured: it asserts that the
front's `/`, `/terms` and `/meta` carry no `Strict-Transport-Security`, and a
control in the same test adds one to a copy of the configuration and requires
the assertion to fail against it.

## The non-commercial notice

The instance is gated non-commercial use only, which is a statement about this
one running deployment and **not** a licence change: the workspace and the
clients stay `MIT OR Apache-2.0`, `crates/selvaged` stays `FSL-1.1-MIT`, and the
specification's prose, schema and vectors stay `CC-BY-4.0`.

The notice is served by the front, in two places:

- a banner appended to the page's own HTML (before `</body>`), so a visitor
  reaches the terms without having to look for them;
- `GET /terms`, answered by the front, which is the full text and the stable URL
  the banner links to.

The banner lives in `proxy/conf.d/default.conf`, because it is a response the
front changes and nothing else can. The page is `proxy/www/terms.html`, a file in
the front's image, because a page of prose is a file and an nginx directive is
not a place to keep one. Its styling is inline and self-contained, and the palette
is the demo page's own, so `/terms` reads as part of the same thing.

`/terms` and `/terms/` answer with the page, the same bytes either way. The page
keeps the instance's restriction and the software's licences apart on purpose,
because that distinction is the whole of the notice: the instance is
non-commercial, the software is not.

The banner is markup injected into another repository's rendered page, and a page
whose HTML stops ending in `</body>` loses it silently. What is asserted is
therefore the bytes a visitor receives and not the files that produce them —
`check-terms.sh` here, without Docker, and the isolated proof below on the box. If
`web_client` ever carries the notice itself, the substitution goes and `/terms`
stays.

## Deploy

The box runs a self-contained copy of this directory, owned by `selvage` (uid
1000, in the `docker` group):

```text
/srv/selvage/            selvage:selvage 0755
  compose.yaml, proxy/   from this directory
  .env                   selvage 0600, from .env.example
  tls/                   root:root 0750
    origin.pem           root:root 0644
    origin.key           root:101  0640
```

Every hand command runs as `selvage` from `/srv/selvage`, with no `sudo` and no
`-f`: Compose reads `compose.yaml` and `.env` from the working directory, and the
file's own `name:` makes the project `selvage-prod`. The timer and
`selvage-deploy` run Compose the same way, so a `compose.override.yaml` put
beside it applies to all three.

| To | Run |
|---|---|
| apply an edited `.env` | `docker compose pull && docker compose up -d` |
| restart one service | `docker compose restart <service>` |
| deploy a change to `proxy/` | `docker compose up -d --build proxy` |
| see what runs | `docker compose ps`, `docker compose images` |
| take the demo down | `sudo systemctl stop selvage-update.timer`, then `docker compose down` |
| bring it back | `docker compose up -d`, then `sudo systemctl start selvage-update.timer` |

The timer runs `up -d` every five minutes, so a `stop` or `down` made while it
runs is undone on the next tick. Stop the timer first, for one service as much
as for all three.

A plain `pull` also asks the registry for the front's local image and warns that
it cannot find it; `--ignore-buildable` skips that. `up -d` recreates only a
service whose image or configuration changed. The front is built only when its
image is missing, so it moves only with `--build`. Its `depends_on` entries carry
`restart: true`, so recreating `selvaged` or `selvage-web` restarts the front
too. The front resolves those names once, at startup, and a recreated container
can come back on a different address.

Recreating the front drops the WebSockets through it for a second or two, and
rooms survive it. Recreating `selvaged` ends every room, because the server is
memory-only and there is no reclaim after a restart.

### The update timer

`selvage-update.timer` runs `selvage-update.service` every five minutes. The
service runs as `selvage` in `/srv/selvage`:

```sh
docker compose pull --ignore-buildable --quiet && docker compose up -d && docker image prune --force
```

A tick with nothing new recreates nothing. A new `latest` reaches the demo
within five minutes and with no approval, and a new `selvaged` ends every room.
That is the price of following `latest`. Pin a version to opt out, as below. For
`selvaged`, `latest` moves on every release tag of `reference_server` that is
not a prerelease. For `selvage-web`, `web_client`'s image workflow moves it on
every `v*` tag, a prerelease included. The prune removes the dangling images a
recreation leaves behind, because the box has 29 GB of disk.

A tick fails, rather than starting Docker, while `docker.service` is stopped
(`Requisite=`).

```sh
systemctl list-timers selvage-update.timer    # when it last ran and runs next
journalctl -u selvage-update.service -n 50    # what it did
sudo systemctl start selvage-update.service   # one tick now
sudo systemctl stop selvage-update.timer      # stop until the next boot
sudo systemctl disable --now selvage-update.timer   # stop until re-enabled
```

The service and `selvage-deploy` take the same lock, `/srv/selvage/.update.lock`.
A tick that finds a deploy running does nothing, and a deploy waits up to ten
minutes for a tick to finish.

### Pinning a version

`.env` names a tag per image. `latest` follows every release. A version stays
where it is, and every tick leaves it there. A pinned service is behind the CI
approval again, apart from a republish of that same version tag:

```sh
cd /srv/selvage
sed -i 's|^SELVAGED_IMAGE=.*|SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:0.4.6|' .env
docker compose pull && docker compose up -d
```

To unpin, write `:latest` back and run the same two commands. The CI dispatch
below makes the same edit: `server_version=0.4.6` pins and
`server_version=latest` unpins.

### Rollback

Pin the previous version. `selvage-deploy` keeps the `.env` it replaced as
`/srv/selvage/.env.prev`, so after a CI deploy `cp .env.prev .env` followed by
the two commands also goes back. A deploy that *failed* changed neither file.
Its references are left in `.env.deploy`, and the next tick returns to what
`.env` names. Rolling back `selvaged` ends every live room a second time.

The front has no version of its own. An older front is `proxy/` at an older
commit, followed by `up -d --build proxy`.

### Installing

From a checkout of `reference_server`, only the files the box runs:

```sh
box=selvage@selvage-protocol-prod
ssh "$box" 'sudo install -d -o selvage -g selvage -m 0755 /srv/selvage'
git archive <sha> packaging/prod/compose.yaml packaging/prod/.env.example packaging/prod/proxy \
  | ssh "$box" 'tar -x -C /srv/selvage --strip-components=2'
ssh "$box" 'cd /srv/selvage && cp -n .env.example .env && chmod 600 .env
  sudo install -d -o root -g root -m 0750 tls'
# then origin.pem and origin.key into tls/, and the key's permissions (below)
```

The units, then the timer:

```sh
scp packaging/prod/selvage-update.service packaging/prod/selvage-update.timer "$box:"
ssh "$box" 'set -e
  sudo install -o root -g root -m 0644 selvage-update.service selvage-update.timer /etc/systemd/system/
  rm selvage-update.service selvage-update.timer
  sudo systemctl daemon-reload
  sudo systemctl enable --now selvage-update.timer'
```

`selvage-deploy` and its sudoers line are under *Deploying a release from CI*.

### Moving from `/etc/selvage`

The box first ran from `/etc/selvage`, with root's Compose and digest pins. The
move keeps the project name, so one `up -d` from the new directory recreates the
three containers in place. That step is the only downtime, about 20 seconds,
most of it the old front closing its connections.

1. `docker compose pull --help | grep ignore-buildable`, as `selvage`. The timer
   needs that flag. Note the versions running now, from each container's
   `org.opencontainers.image.version` label.
2. Install `/srv/selvage` as above, but copy `tls/` with
   `sudo cp -a /etc/selvage/tls /srv/selvage/`, which keeps the key's owner and
   mode. Then pin both lines of `.env` to the versions step 1 found, so the move
   is not also an upgrade:
   `sed -i 's|^SELVAGED_IMAGE=.*|SELVAGED_IMAGE=ghcr.io/selvage-protocol/selvaged:<version>|' .env`,
   and the same for `SELVAGE_WEB_IMAGE`.
3. Prepare in `/srv/selvage`, which changes nothing running: `docker compose config -q`,
   `docker compose pull --ignore-buildable` and `docker compose build proxy`.
   Without the buildx plugin, Compose warns that it cannot use Bake and builds
   with the daemon's own BuildKit; `DOCKER_BUILDKIT=0` is the fallback if that
   fails. Then `docker compose run --rm --no-deps --entrypoint /usr/sbin/nginx proxy -t`,
   a throwaway front with the real `./tls/` binds and no published port, whose
   `-t` reads the certificate and key as the front will. It resolves `selvaged`
   and `selvage-web`, so run it while the old containers are up. If it cannot
   mount `tls/`, run `sudo chgrp selvage /srv/selvage/tls` and try again; the
   key stays `root:101 0640`.
4. `docker compose up -d`. This is the cutover.
5. Check it. `docker compose ps` should show three `running` services, and the
   front's mounts should now name `/srv/selvage/tls`. The origin read:
   `curl -sk -H 'Host: selvage-demo.dontblameme.dev' https://127.0.0.1/meta`.
6. Install the new `selvage-deploy`, then the units and the timer.
7. Once it has run a day, retire `/etc/selvage`. It holds the only other copy of
   the key.
8. Unpin, if that is the plan, as its own observed change.

To go back, until step 7: `sudo systemctl disable --now selvage-update.timer`
if step 6 installed it, then `cd /etc/selvage && sudo docker compose up -d`.
Stop the timer first. Both directories are the project `selvage-prod`, so a
tick would recreate the containers from `/srv/selvage` again, and its prune
can delete the images the old `.env` names.

## Cloudflare answers a datacenter client with a challenge

`curl https://selvage-demo.dontblameme.dev/meta` from a GitHub runner, and from this
box, is refused at the edge:

```console
HTTP/2 403
cf-mitigated: challenge
server: cloudflare
<title>Just a moment...</title>
```

It is a **managed challenge**, a zone-level Cloudflare setting aimed at
automated traffic, and a programmatic client cannot pass it: there is no
browser to run the challenge's script and no `cf_clearance` cookie to carry.
What triggers it here is the *address*, not the request — the two reads that
answer are the box's own front (`curl -sk -H 'Host: selvage-demo.dontblameme.dev' https://127.0.0.1/meta`, 200 with the right body) and the same public read from a
residential host (200). The origin
and the front are healthy; the edge is the layer saying no.

**What that means:** nothing on a datacenter address can read this deployment's
public URL, so no CI job can assert against it — not `curl` in a workflow, not
`curl` on the box. The honest source for what is running is the origin read
through the front on the box, and that is what
`.github/workflows/deploy-prod.yml` asserts over the Tailscale SSH path its
deploy step already opened. Its read of the public URL is read in three shapes:
a challenge, an unreadable read and any other answer that is not a version are
**reports** — each says in words what came back, a challenge ends the attempt at
the first challenge response rather than polling a deadline it cannot pass, and
none of them turns a healthy deploy red, because an edge is not something a deploy
can fix — while a 200 reporting a version *other* than the one the dispatch named
is **red**, because there the origin is right and the edge is serving something
this deploy did not put behind it. A Cloudflare bypass, a `cf_clearance` cookie
kept anywhere, or a change to the zone's bot settings are all the wrong answer to
this, and the next reader should not spend a cycle finding that out again.

## Deploying a release from CI

`.github/workflows/deploy-prod.yml` in `reference_server`: a manual dispatch that
joins the tailnet, hands this box one request over Tailscale SSH and then asserts,
over that same SSH path, that the origin is serving the version it deployed —
`https://127.0.0.1/meta` through the front, naming the demo's own `Host` because a
loopback address is where the front is and not a name it answers, and the page
answering 200. The
public URL is read too: a challenge, an unreadable read or any other answer that
is not a version is a report, and a 200 reporting a version other than the one
the dispatch named fails the run (the challenge above is why the first three
cannot fail it). `server_version=latest` names no version, so `/meta` is read
and not compared, as for a page-only dispatch.

```sh
gh workflow run deploy-prod.yml --ref main -f server_version=0.4.6
gh workflow run deploy-prod.yml --ref main -f web_version=latest
gh workflow run deploy-prod.yml --ref main -f server_version=latest -f web_version=latest
```

An input it is not given means **leave that line of `.env` exactly as it is**,
so a page release and a server release are separable, a version pins, `latest`
unpins, and a rollback is a dispatch with an older version. The run then waits
on the GitHub environment `prod`, whose required reviewer is the owner: the
approval *is* the moment the credential exists, and it is the moment the
live-session cost above is chosen, for what CI does. It is not a gate on a
service `.env` leaves on `latest`: the timer follows a new `latest` with no
approval at all (*The update timer*, above).

What a run may change is bounded by construction rather than by convention. It
hands the box one tag reference per image and nothing else. `deploy.py` matches
every value against one fixed pattern, `ghcr.io/selvage-protocol/<image>:<tag>`,
and rewrites those lines of `/srv/selvage/.env` and no other file. A run cannot
add a port, drop a capability, mount a certificate or raise a memory limit.
Anything else it might want is a pull request and a hand install.

The request stays a tag and is not resolved to a digest. `.env` names a tag so
that the timer can follow it, and pinning here means writing a version tag. What
a tag named when the box pulled it is printed in the deploy log, and later
`docker image inspect --format '{{join .RepoDigests " "}}' <reference>` reads it
back.

A run that fails also leaves `/srv/selvage/.env` alone. The references it was
deploying live in `/srv/selvage/.env.deploy` while it works, and `.env` is
written only once the containers are up and converged. It is the input for the
next tick and the next hand command, so one bad release cannot leave a box that
a later `up -d` reproduces. A deploy holds the timer's lock from start to end.

Getting in is Tailscale SSH as `deployci`, a local user on the box with no
password, no key, no group but its own, and exactly one permitted root command —
`/usr/local/sbin/selvage-deploy ""`, where the `""` is what tells sudo that no
argument is allowed rather than any. `sudo -l -U deployci` on the box is the
whole of the privilege model, and `deployci.sudoers` is the tracked copy of that
line. The account `selvage` is deliberately not used: it has blanket
passwordless sudo, so a shell as `selvage` is a shell as root, and lending that
to CI would make the deploy credential a root credential.

Installing the two files by hand, from a checkout at the merged commit, so the
repository and the box agree:

```sh
box=selvage@selvage-protocol-prod
scp packaging/prod/deploy.py         "$box:/tmp/selvage-deploy"
scp packaging/prod/deployci.sudoers  "$box:/tmp/selvage-deploy.sudoers"
ssh "$box" 'set -e
  sudo install -o root -g root -m 0755 /tmp/selvage-deploy /usr/local/sbin/selvage-deploy
  sudo visudo -c -f /tmp/selvage-deploy.sudoers
  sudo install -o root -g root -m 0440 /tmp/selvage-deploy.sudoers /etc/sudoers.d/selvage-deploy
  rm -f /tmp/selvage-deploy /tmp/selvage-deploy.sudoers
  sudo -l -U deployci'
```

The installed name has no `.py`: `/usr/local/sbin/selvage-deploy` is the path the
sudoers rule names, and the shebang makes it the program. `visudo -c` reads it
before it is installed, so a syntax error cannot lock the box out of `sudo`.

The user itself is separate, because it is not a file:

```sh
ssh "$box" 'sudo adduser --system --group --home /var/lib/selvage-deploy --shell /bin/bash deployci'
```

`deployci` needs a real shell because Tailscale SSH runs the login shell, and
`/usr/sbin/nologin` would refuse the session rather than the command. It is
created with no password (`--system`) and no `authorized_keys`, so the tailnet
policy is the only way in. That policy is not in this repository: the runbook in
`ai_notes` owns it.

### What a page-only dispatch does not assert

A dispatch naming only `web_version` leaves the verification at the page itself:
the origin's `/` must answer 200 and `/meta` must be readable, the page container
is asserted to run the image its tag pulled, and the public read compares
nothing, because there is no server version in it. **It cannot tell which page
build is being served**, and that is worth knowing plainly rather than
discovering.

The page has no version read to lean on. `dist/index.html` carries no version, and
the only version-shaped string in the page is `web_client/0.1.0` inside
`dist/app.js` — the same on every build, and a separate staleness in
`web_client`'s own release rather than a handle here. The two candidates for an
expectation both fail on their own terms:

- **The committed `dist/`** is not something this repository can hold as an
  expectation. The deploy workflow checks out `reference_server`, not `web_client`,
  so it would need that repository at the released tag as a second source; and the
  checkout's own working tree would lie the moment `main` moved past the tag.
- **The image's own bytes**, read anonymously from the registry, would be honest
  and cheap enough — the whole page image is under 10 MB of layers for 0.2.1 — but a
  byte comparison has to name a file, and **the bytes a visitor receives are
  rewritten twice**: the front's `sub_filter` injects the non-commercial notice into
  `</body>`, and Cloudflare Rocket Loader rewrites `index.html` in flight, which is
  why the delivered HTML is not byte-reproducible through that edge. Only a file
  neither hop rewrites could be compared, and a production deploy step pinned to
  "this file happens not to be rewritten" is a coupling to another repository's
  asset layout, able to fail only for a reason the deployment does not own.

What a byte comparison would prove is asserted around it, by the repository that
owns each half. `web_client`'s `publish` job runs `scripts/assert-image-page.sh`
against the image it has just pushed, and that asserts the version label and then,
per architecture, that a *running* image serves `/index.html`, `/app.js`,
`/site.webmanifest` and a content-hashed chunk with the sha256 of the committed
`dist/` (`scripts/check-page.sh`); this deployment's `deploy.py` asserts the page
container runs the image the requested tag pulled, and prints its digest. The
reads that remain worth making are the ones that are made: the container runs
what was named, and the front answers its page.

**The honest way to give the page a version read is to publish one as a value** —
a `version.json`, or a `<meta>` in the built page, written by `web_client`'s build
at release time and read on the box through the front the way `/meta` is. A read
survives both rewrites; a byte comparison does not. Until that exists, a page-only
dispatch is verified weakly on purpose: it asserts that the front serves a page,
not which build that page is.

## Certificate

The certificate is a Cloudflare **Origin Certificate**, and the front presents it
to Cloudflare on every connection to this listener. It has to cover the host the
zone serves, `selvage-demo.dontblameme.dev`, because **Full (strict)** validates
the origin's certificate against the name the visitor asked for: a certificate
that does not name it is answered at the edge with Cloudflare's **526** and
never reaches the front. A `*.dontblameme.dev` Origin Certificate covers this
host, and every other single-label name on the zone, in one file — the landing
page's `selvage.dontblameme.dev` among them. It covers *that* and no more: a
wildcard matches one label, so a nested name such as `a.b.dontblameme.dev` is
not covered by it, and a host serving one needs a certificate that names it,
either its own or an entry on this certificate's SAN list.

It lives at `/srv/selvage/tls/origin.pem` and `/srv/selvage/tls/origin.key`.
Compose mounts both by relative path, at the path inside the front that
`proxy/conf.d/default.conf` names, so a replacement is a file copy and
`docker compose restart proxy` — never a rebuild.

The key is mode **640**, owned `root` and group **101** on the host, which is the
group uid 101 is in inside the front's image. The front is the only thing that
reads it: the page container has no mount, the server has no mount, and the
`messagebus` group that also owns gid 101 on the host cannot reach the file
through `/srv/selvage/tls`, which is mode 750 `root:root`. `selvage` cannot read
it either: its Compose names the path and the daemon does the reading. Server private keys
are never printed, copied into this directory, or checked in.

## The isolated proof

`check-terms.sh` is the half of it that runs anywhere, Docker or not: it starts
the front's own `nginx.conf` and `conf.d/default.conf` under nginx from `PATH`
(or from nixpkgs when there is none) with the two upstream names answered on
loopback and TLS off, and reads the notice back out of the bytes it serves.
`/terms` and `/terms/` must answer with the file in this repository and each with
the same bytes; the served page's text must still carry every claim the notice
makes and every licence it names; every link target must be there; and the banner
must still arrive in the page's own bytes. What the harness rewrites to run at
all is asserted too, so a change to the front's shape stops the script instead of
being quietly substituted into a passing run. It proves the notice reaches a
visitor and nothing about TLS, the certificate or the upstreams, which is what
the box proof below is for.

`test_front_limits.py` is the same harness pointed at the limits, and runs in CI
as the `prod-front` check. It asserts what a configuration file cannot: that a
request naming a Host the front is not the server for is closed rather than
answered, that `/session` is refused with a real `429` past its burst, that six
concurrent `/meta` reads are refused past `permeta`'s four, and that a source
which has just spent `/meta` is still served on `/session`. Each client is a
loopback address of its own, because the zones are keyed on
`$binary_remote_addr`, so none of the claims waits for a rate to refill, and every
request carries the name the front answers, read out of the configuration under
test rather than repeated in the harness. Each claim that is a negative carries
its own control in the same test — a copy with the host guard removed, a copy
whose `/meta` names the session request zone, a copy with an HSTS field added —
and a control that passed would mean the test could not see the defect it exists
for.

The front's TLS path, its routing and its log policy are proved without starting
this deployment, against throwaway backends on a high port, rather than assumed
from the configuration — the exact commands and their real output are in
`ai_notes/.tmp/prod-deploy-prep-2026-09-21.md`. What the proof covers: a TLS
handshake that presents the origin certificate for the right name and refuses
1.0 and 1.1; `/` answered by a `selvage-web` container with the terms banner in
the bytes; `/meta` answered by a `selvaged` container; `/terms` answered from the
front's own image; a WebSocket upgrade answered `101`; `429` from both per-source
caps under load; a forged `CF-Connecting-IP` from a peer outside Cloudflare's
ranges ignored; a dead upstream answered in five seconds; and the front's whole
log, which is empty. The level that keeps it empty is proved load-bearing by the
same traffic against a second container with `error_log` at `error`, where the
token appears 28 times.
