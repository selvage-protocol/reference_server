# The public demo's shape

The exact files that run `selvage.dontblameme.dev`, tracked here so the
deployment's shape is a reviewable artefact in a repository rather than a
compose file that exists only on a machine. `pi-demo/` beside this directory is
the same idea for the Pi.

The two shapes differ in where the front is. The Pi is tailnet-only, so its
front is a TLS terminator and nothing else: `selvaged` serves the page itself
with `--serve-page`, and the front relays every path to it. Here the origin is
public and the owner's decision of 2026-09-21 was a page container of its own, so
this front terminates TLS *and* routes, and `selvaged` serves `/session` and
`/meta` alone.

The live state — the host, the images running, the network security group, the
upgrade procedure — is `ai_notes/docs/runbook-prod-demo.md` (the project's own
notes, not a path a reader of this repository can open). That document owns
those facts; this directory owns the files, and the two must agree.

| File | Installs to | What it is |
|---|---|---|
| `compose.yaml` | `/etc/selvage/compose.yaml` | The three services, the hardening and the memory limits |
| `.env.example` | `/etc/selvage/.env` | The two image references the next `up` runs |
| `deploy.py` | `/usr/local/sbin/selvage-deploy` | The deploy: reads one request on stdin, verifies the shape, pulls and ups |
| `deployci.sudoers` | `/etc/sudoers.d/selvage-deploy` | The one root command the CI user on this box may run |
| `test_deploy.py` | nowhere; run where it is | What the request grammar refuses, and that a shape mismatch touches nothing |
| `proxy/Dockerfile` | build context `/etc/selvage/proxy/` | The front's image, from the two files below it |
| `proxy/nginx.conf` | baked into that image | Process and http scope: the log policy, the temp paths, the per-source zones |
| `proxy/conf.d/default.conf` | baked into that image | The server block: TLS, the routes, the limits, the terms page |
| `proxy/conf.d/cloudflare-ips.conf` | baked into that image | Cloudflare's published ranges, for `real_ip` |

## The shape

One origin serves everything, one port is published, and the split is by path:

```text
              browser
                 |  https://selvage.dontblameme.dev/...
                 v
          Cloudflare  (proxied DNS, Full (strict), redirect at the edge)
                 |  TCP 443, from Cloudflare's published ranges only
                 v
    +---------------------------------  Azure Standard_B2ats_v2
    |  proxy            (nginx, uid 101, port 443 -> 8080, TLS)
    |    /session  ------------------>  selvaged   :8080   WebSocket
    |    /meta     ------------------>  selvaged   :8080   JSON
    |    /terms    ------------------>  answered by the front itself
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

Rules 300 and 320 are the two rules the subscription created, kept and turned
into a denial rather than deleted, so the change is legible against what was
there. The one fact to keep in step with `proxy/conf.d/cloudflare-ips.conf` is
the range list itself: refresh both together from
`https://api.cloudflare.com/client/v4/ips`, which is where both lists came from.
Port 22 is not in the NSG at all and is refused by the default rule; hands-on
access is Tailscale SSH over the tailnet, which this change does not touch.

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

The box has 2 vCPU, 892 MiB of RAM and **no swap**. Three containers and the
Docker daemon have to fit in that, so the server is told what it may hold rather
than left on its reference defaults, which assume headroom this host does not
have (at the defaults, `--max-connections × --outbound-queue-bytes` is 32 GiB).

| Flag | Value | Why this one |
|---|---|---|
| `--max-connections` | 32 | Multiplies the per-connection queue, so it is the term that sizes the process: 32 × 8 MiB is a 256 MiB outbound ceiling, the largest surprise this host can absorb. A demo may have 32 live sockets; the server's own README ("Sizing a box") offers 32 as a defensible set for a box around this size. |
| `--outbound-queue-bytes` | 8388608 | The server refuses anything below one whole frame, and one whole frame is 8 MiB (`net::MAX_FRAME_BYTES`). 8 MiB is therefore the smallest queue this configuration accepts, not a choice among larger ones. |
| `--max-rooms` | 64 | Rooms held at once. More than a demo needs, and each one's documents are the other unbounded term, so it is bounded here. |
| `--max-peers-per-room` | 8 | One room is a session with a friend; 8 peers × 256 paths is the coupling the README warns pairs with the queue. |
| `--max-documents-per-room` | 256 | The document set is echoed whole to every peer on `doc.open`/`doc.close`, so lowering it is what lowers the echo — and it is what makes the 8 MiB queue the floor rather than something higher. |
| `--max-envelope-bytes` | 5242880 | The default, unchanged on purpose: the widest legal request is a `doc.grant` carrying 4 MiB of paths, and a smaller bound would refuse a legal one. |
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

The residual, named: 32 connections × 8 MiB is 256 MiB of outbound queue, so a
server under deliberate abuse could reach most of its own 320 MiB cap before
the container is the thing that dies. That is the designed outcome and it is
bounded — the room state goes with it — but it is not free, and the lever if it
ever matters is `--max-connections`.

## What the front does that the server cannot

`PROTOCOL.md` §12 requires a public deployment to supply a connection cap, an
idle deadline and a rate limit in front of the server, and requires that a
deployment not log request URLs. The split is the one `ai_notes` settled when the
capacity flags landed:

- **Per-source caps at the front.** `selvaged` has no per-source view at all.
  `limit_conn` allows one source 32 connections to the page, 8 concurrent
  `/session` sockets and 4 concurrent `/meta` reads, each in its own zone so one
  endpoint's count cannot spend another's; `limit_req` allows 5 `/session`
  handshakes a second with a burst of 10. Past either the source gets a **429**.
  The keys are the end client's address: `real_ip` is told to trust
  `CF-Connecting-IP` from Cloudflare's ranges and from nowhere else, so a
  connection that did not come through Cloudflare cannot name its own source.

- **Total caps at the server.** `--max-connections` and the rest, above.

- **Idle deadline.** No in-process idle reaper exists and §2.1 forbids one for a
  seated session. The front's `proxy_read_timeout` is 300 s, an order of
  magnitude above the server's 30-second ping, so it only ever fires on a socket
  that is genuinely gone; Cloudflare's own 100-second idle close is above the
  ping too. **Nothing here lengthens the ping interval**, and nothing should:
  30 s is what keeps a live session inside Cloudflare's 100 s.

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

The consequence is worth stating plainly: **in normal operation the front logs
nothing at all.** Its whole `docker logs` is empty until something at `crit`
happens. That is the posture, not an oversight, and the things an operator normally
reads the front's log for are answered elsewhere — `docker compose ps` for whether
it is up, the server's own startup line for the limits in force, and the isolated
proof below for whether a change broke routing.

The level is load-bearing rather than decorative, and that is tested rather than
asserted: the isolated proof runs a second container from the same image with only
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

Both live in `proxy/conf.d/default.conf`, because the notice is about the
instance and this front is where the instance's configuration lives — the page
itself is `web_client`'s built bundle. The cost of injecting markup into another
repository's rendered page is real and is named rather than hidden: a page whose
HTML stops ending in `</body>` loses the banner silently, so the substitution is
asserted against the **served bytes** in the isolated proof below and not
against this file. If `web_client` ever carries the notice itself, the
substitution goes and `/terms` stays.

## Deploy

From this directory, on a host that already has the two files installed:

```sh
sudo docker compose -f /etc/selvage/compose.yaml up -d
```

One command: it builds the front from `/etc/selvage/proxy/`, pulls whatever
`.env` names, creates the network and starts the three services.

Two settings make that one command honest about a change:

- the front has `pull_policy: build`, so `up` rebuilds it even when the image
  already exists. Without it a configuration change would sit in this repository
  and not in the running container, because Compose does not rebuild for changed
  build-context content on its own;
- the front's `depends_on` entries carry `restart: true`, so recreating
  `selvaged` or `selvage-web` restarts the front too. It resolves those two names
  once, at startup, and a recreated container can come back on a different
  address — a proxy left running would go on dialling the old one.

**That rebuild has a cost worth knowing before you run the command by hand.**
With BuildKit's default attestations the built image's manifest — and so its
image ID — is stamped per build even when `proxy/` has not changed at all, and
Compose recreates any container whose image ID moved. So this command replaces
the front *every* time, which drops the WebSockets through it and makes the origin
unreachable for a second or two. Rooms survive: `selvaged` is not recreated, and
the clients reconnect on their own. Three builds of one context on this box,
with nothing edited between them:

    sha256:702235e5…  sha256:74fb7a39…  sha256:6f4c1cdd…

`deploy.py` sets `BUILDX_NO_DEFAULT_ATTESTATIONS=1` for the Compose it runs, which
makes that rebuild content-addressed instead — the same three builds then produce
one image ID — so a deployment whose images have not changed recreates nothing at
all. That is why the automated path is quieter than this command, and the only
reason the two differ. The front's image is local and nothing is published from
it, so the attestations are worth nothing here; Compose's own `build.provenance: false` was tried first and did not take effect on Compose 2.40.3.

Bootstrap, once, from a checkout of `reference_server`:

```sh
git archive <sha> packaging/prod | ssh selvage@selvage-protocol-prod 'sudo tar -x -C /etc/selvage --strip-components=2 packaging/prod'
# then: /etc/selvage/.env from .env.example, and the key's permissions (below)
```

### Moving an image

`.env` names the images. Resolve a release version to its index digest, put
`ghcr.io/selvage-protocol/selvaged@sha256:…` in `/etc/selvage/.env`, and run the
`up -d` above. **A digest, not a tag**: a tag is a name someone else can repoint,
and this is the public origin.

```sh
scripts/image-digest.py ghcr.io/selvage-protocol/selvaged:0.2.0
```

That is the same anonymous pull flow `assert-multiarch-layers.py` uses
(`scripts/registry.py`), and it is what the deploy workflow calls. By hand, the
one line of it a deploy needs is:

```sh
repo=selvage-protocol/selvaged; tag=0.2.0
tok=$(curl -s "https://ghcr.io/token?scope=repository:$repo:pull&service=ghcr.io" \
      | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
curl -sI -H "Authorization: Bearer $tok" \
     -H 'Accept: application/vnd.oci.image.index.v1+json' \
     "https://ghcr.io/v2/$repo/manifests/$tag" | tr -d '\r' \
  | grep -i docker-content-digest
```

`docker compose up -d` is safe to run while the demo is live, and it does not
preserve sessions: `selvaged` is memory-only, so a recreate ends every room and
a guest sees the room gone. There is no reclaim after a restart.

### Rollback

`.env` holds one generation and `deploy.py` keeps the file it replaced as
`/etc/selvage/.env.prev`, so rolling back by hand is restoring that file and
running the `up -d` above. The automated path is a dispatch of the same workflow
with the older `server_version` — the same mechanism as a deploy, which is why it
is exercised by every deploy rather than rotting until the day it is needed.

Either way, rolling back `selvaged` ends every live room a second time. That is
what the approval gate is for. The front has no version of its own to roll back
to and does not need one: it is built from the files in this directory, so an
older front is that directory at an older commit and the same `up -d`.

## Deploying a release from CI

`.github/workflows/deploy-prod.yml` in `reference_server`: a manual dispatch that
joins the tailnet, hands this box one request over Tailscale SSH and then checks
the *public* origin reports the version it deployed.

```sh
gh workflow run deploy-prod.yml --ref main -f server_version=0.2.1
gh workflow run deploy-prod.yml --ref main -f web_version=0.1.1
gh workflow run deploy-prod.yml --ref main -f server_version=0.3.0 -f web_version=0.2.0
```

An input it is not given means **leave that service exactly as it is**, so a page
release and a server release are separable and a rollback is a dispatch with an
older version. The run then waits on the GitHub environment `prod`, whose
required reviewer is the owner: the approval *is* the moment the credential
exists, and it is the moment the live-session cost above is chosen.

What a run may change is bounded by construction rather than by convention. It
handed the box two digest-pinned references and the sha256 of `compose.yaml`,
and nothing else: `deploy.py` matches every value against one fixed pattern and
**verifies** the compose file against that hash instead of writing it. A run
cannot add a port, drop a capability, mount a certificate or raise a memory
limit, and it cannot rewrite the shape to do so later. Anything else it might
want is a pull request and a hand install.

Getting in is Tailscale SSH as `deployci`, a local user on the box with no
password, no key, no group but its own, and exactly one permitted root command —
`/usr/local/sbin/selvage-deploy`, with **no argument wildcard** in the sudoers
rule, which is why the request arrives on stdin. `sudo -l -U deployci` on the box
is the whole of the privilege model, and `deployci.sudoers` is the tracked copy
of that line. The account `selvage` is deliberately not used: it has blanket
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

## Certificate

The certificate is a Cloudflare Origin Certificate for
`selvage.dontblameme.dev`, valid to 2041. The owner placed it at
`/etc/selvage/tls/origin.pem` and `/etc/selvage/tls/origin.key` and the front
reads both by path, so a replacement is a file copy and `docker compose restart proxy` — never a rebuild.

The key is mode **640**, owned `root` and group **101** on the host, which is the
group uid 101 is in inside the front's image. The front is the only thing that
reads it: the page container has no mount, the server has no mount, and the
`messagebus` group that also owns gid 101 on the host cannot reach the file
through `/etc/selvage/tls`, which is mode 750 `root:root`. Server private keys
are never printed, copied into this directory, or checked in.

## The isolated proof

The front's TLS path, its routing and its log policy are proved without starting
this deployment, against throwaway backends on a high port, rather than assumed
from the configuration — the exact commands and their real output are in
`ai_notes/.tmp/prod-deploy-prep-2026-09-21.md`. What the proof covers: a TLS
handshake that presents the origin certificate for the right name and refuses
1.0 and 1.1; `/` answered by a `selvage-web` container with the terms banner in
the bytes; `/meta` answered by a `selvaged` container; `/terms` answered by the
front; a WebSocket upgrade answered `101`; `429` from both per-source caps under
load; a forged `CF-Connecting-IP` from a peer outside Cloudflare's ranges
ignored; a dead upstream answered in five seconds; and the front's whole log,
which is empty. The level that keeps it empty is proved load-bearing by the same
traffic against a second container with `error_log` at `error`, where the token
appears 28 times.
