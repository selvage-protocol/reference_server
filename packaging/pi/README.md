# The Pi demo's container shape

The two containers that run the demo on `lumi-raspberrypi` — a Raspberry Pi 5,
aarch64, at `100.64.0.3` on the tailnet — tracked here so that a deployment
artefact has a reviewable home instead of existing only on the machine.
`packaging/prod/` beside this directory is the same idea for the public demo, and
`packaging/pi-demo/` holds the native shape these two replaced.

The live state — the URL, the images running, the upgrade procedure — is
`ai_notes/docs/runbook-pi-demo.md` (the project's private notes, not a path a
reader of this repository can open), and the release procedure for every
deployment is the release runbook `ai_notes/docs/runbook-release.md`. Those
documents own the live facts; this directory owns the files, and the two must
agree.

| File | Installs to | What it is |
|---|---|---|
| `compose.yaml` | `/root/selvage-compose/compose.yaml` | The two services, the hardening and the tailnet publishes |
| `.env.example` | `/root/selvage-compose/.env` | The two image references the next `up` runs |
| `deploy.py` | `/usr/local/sbin/selvage-pi-deploy` | The deploy: reads one request on stdin, verifies the shape, pulls and ups |
| `deployci.sudoers` | `/etc/sudoers.d/selvage-deploy-pi` | The one root command the CI user on this box may run. **Neither it nor its user exists on the Pi today** — see below |
| `watchtower/compose.yml` | `/home/pi/docker/watchtower/compose.yml` | The host's image updater, tracked for the one thing it needed: a pin |
| `test_deploy.py` | nowhere; run where it is | What the request grammar refuses, that a shape mismatch touches nothing, and that the shape and the deploy agree about what is deployed |

## The shape

Two containers, two ports, both published on the tailnet address and nowhere
else. Nothing here is a front: the tailnet is the boundary, so the page and the
server each listen where a guest can reach them directly.

```text
                        tailnet (the boundary)
                                 |
        +------------------------+-------------------------+
        |        lumi-raspberrypi, 100.64.0.3, a Pi 5       |
        |                                                  |
        |   :80     selvage-web   ->  the page             |
        |   :8080   selvaged      ->  /session and /meta   |
        +--------------------------------------------------+
```

The ports are the shape the desktop clients already assume: both default to
`ws://100.64.0.3:8080` for the server, so the session server kept the address the
native deployment had. The page is the published `selvage-web` image rather than
the page baked into the server image, which is why `selvaged` runs without
`--serve-page` here — a visitor's page and the server are one release each.

Both containers run read-only, with every capability dropped and no process able
to gain one, and neither mounts anything: the server is memory-only and the page
image moves nginx's writable paths onto the runtime's own tmpfs. There are no
memory limits, deliberately: the Pi has 8 GiB and two containers that fit in it
without being told.

## What a deploy may change

Only which of two published images runs: one digest-pinned reference each, names
matching `ghcr.io/selvage-protocol/(selvaged|selvage-web)@sha256:…` and nothing
else. The compose file is *verified* against the sha256 the request names and
never written, so a request cannot add a port, publish another address, drop a
capability or change a command. The grammar, the staging and the convergence
check are the public demo's, in `packaging/pi/deploy.py`; the properties are
asserted in `packaging/prod/test_deploy.py`, which `test_deploy.py` here runs
against this box's module.

## The auto-update, bounded

The host runs `watchtower` beside this stack, from its own compose file, with the
Docker socket mounted. It is unscoped, so it watches every container on the host,
and its image was `nickfedor/watchtower` with no tag. Watchtower updates the
containers it watches and its own container is among them, so before this shape
was tracked the Pi followed whatever `latest` had become, with the socket
attached, and nothing in this project had proved the image that arrived.

Two things bound it, and neither changes what runs today beyond the pin itself:

- `watchtower/compose.yml` pins the image it is already running, by digest;
- both services in `compose.yaml` carry
  `com.centurylinklabs.watchtower.enable=false`, which watchtower honours whether
  it is run scoped, label-filtered or unscoped, so this stack can only be moved
  by `deploy.py`.

Everything else on the host is left exactly as it was: this instance still
watches the owner's other containers, which is their arrangement and not this
project's to change.

## Deploying by hand

From this directory, on a host that already has the files installed:

```sh
sudo docker compose -f /root/selvage-compose/compose.yaml up -d
```

Bootstrap or upgrade of the files, from a checkout at the merged commit:

```sh
pi=root@lumi-raspberrypi
scp packaging/pi/deploy.py         "$pi:/tmp/selvage-pi-deploy"
scp packaging/pi/deployci.sudoers  "$pi:/tmp/selvage-pi-deploy.sudoers"
scp packaging/pi/compose.yaml      "$pi:/tmp/compose.yaml"
ssh "$pi" 'set -e
  install -o root -g root -m 0755 /tmp/selvage-pi-deploy /usr/local/sbin/selvage-pi-deploy
  install -o root -g root -m 0644 /tmp/compose.yaml /root/selvage-compose/compose.yaml
  visudo -c -f /tmp/selvage-pi-deploy.sudoers
  install -o root -g root -m 0440 /tmp/selvage-pi-deploy.sudoers /etc/sudoers.d/selvage-deploy-pi
  rm -f /tmp/selvage-pi-deploy /tmp/compose.yaml /tmp/selvage-pi-deploy.sudoers
  sudo -l -U deployci'
```

The installed name has no `.py`: `/usr/local/sbin/selvage-pi-deploy` is the path
the sudoers rule names, and the shebang makes it the program. A box that has no
`.env` yet gets one from `.env.example` before its first `up -d`.

The `sudo -l -U deployci` at the end needs the account to exist, and that is the
one piece which is not a file:

```sh
ssh "$pi" 'adduser --system --group --home /var/lib/selvage-deploy --shell /bin/bash deployci'
```

`deployci` needs a real shell because Tailscale SSH runs the login shell, and
`/usr/sbin/nologin` would refuse the session rather than the command. It is
created with no password and no key, so the tailnet policy is the only way in —
the same account, set up the same way, as the public demo's.

**A shape change is a hand install, and that includes this one.** The compose
file now interpolates both image references out of `.env` instead of naming them
inline, so the box's copy has to be replaced with this one and its `.env` written
from `.env.example` before the next `up -d`. The two references resolve to the
digests already running, so nothing moves because of that edit; the labels the
services gained are what recreate them, and that recreation is what takes them out
of watchtower's reach. Replacing `selvaged` ends every live room.

### Moving an image

Resolve a release version to its index digest, put the pinned reference in
`/root/selvage-compose/.env`, and run the `up -d` above:

```sh
scripts/image-digest.py ghcr.io/selvage-protocol/selvaged:0.2.1
```

That is the same anonymous pull flow the deploy workflow calls and the same
command `packaging/prod/README.md` documents for the public demo.

### Rollback

`.env` holds one generation and `deploy.py` keeps the file it replaced as
`/root/selvage-compose/.env.prev`, so rolling back by hand is restoring that file
and running the `up -d` above. A run that *failed* changed neither: the
references it was deploying are in `.env.deploy`, and the `up -d` above goes back
to what `.env` still names. The automated path is a dispatch of the same workflow
with the older version.

## Deploying a release from CI

`.github/workflows/deploy-pi.yml` in `reference_server`: a manual dispatch that
resolves the versions it is given to registry digests, joins the tailnet as
`tag:ci`, hands this box one request over Tailscale SSH as `deployci`, and then
reads `/meta` and the page over the tailnet and fails unless the server reports
the version the dispatch named.

It declares the `prod` environment, which is the only tailnet credential this
project has: the federated identity is constrained to
`repo:selvage-protocol/reference_server:environment:prod`, so a job needs that
line to be issued a token the exchange accepts. The approval that guards a
production deploy therefore guards this one too, which is the right shape for a
step that ends every live room.

**The workflow is written and cannot reach the box yet.** What it needs does not
exist, and none of it is a file in this repository:

| What is missing | Where it would go |
|---|---|
| A `tag:ci` source rule reaching `lumi-raspberrypi:22` | The tailnet policy, in the Tailscale admin console's access controls. `tailscale debug netmap` on the Pi shows two rules today: the owner's own devices on every port, and `100.64.3.1-3` on port 22. No rule names `tag:ci`, so a runner's packets are dropped before SSH is asked anything |
| A `tag:ci` principal allowed to SSH to `lumi-raspberrypi` as `deployci` | The same policy file, its `ssh` section. The two rules there name the owner's devices and map `root` to `root`; no rule admits `tag:ci`, and none names `deployci` |
| A `deployci` user on the Pi | Not a file: `adduser --system --group --home /var/lib/selvage-deploy --shell /bin/bash deployci`, the same account the public demo has, created with no password and no key so the tailnet policy is the only way in. `getent passwd deployci` on the Pi finds nothing today |
| The sudoers line bounding that user | `/etc/sudoers.d/selvage-deploy-pi` on the Pi, tracked here as `deployci.sudoers`. There is no line in `/etc/sudoers.d` on the Pi today, so the user would have no privilege at all until it is installed |

Until those exist, a dispatch of `deploy-pi.yml` fails before it reaches the box: the
tailnet drops the runner's packets, so the wait for `deployci` never succeeds. The
request is built on the runner and nothing on the box is touched. The deploy is a
hand `up -d` with a hand-edited `.env` in the meantime, which is what
`ai_notes/docs/runbook-pi-demo.md` records.
