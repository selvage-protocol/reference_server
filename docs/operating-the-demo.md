# Operating the public demo

The demo at <https://selvage-demo.dontblameme.dev> runs the tracked
[`deploy/`](../deploy) shape. For general self-hosting and TLS requirements, see
[Deploying](deploying.md); for page-serving privacy requirements, see
[Serving the page](serving-the-page.md). This guide describes the operational
contract, not a live host inventory. Read image versions, digests, credentials,
firewall rules, and installed files from their authoritative systems.

## Installation and privilege boundary

The Compose project is `selvage-prod`, rooted at `/srv/selvage`. It contains the
TLS front, `selvaged`, and the separate page image. Only the front publishes a
port. The proxy configuration is baked into its locally built image; a proxy
change requires a rebuild, not just a restart.

Initial provisioning is an administrator's operation, not a CI deploy:

1. Provision Docker Compose, the host's firewall, TLS origin certificate, and
   Tailscale SSH access. Restrict origin ingress to the intended edge. Refresh
   the firewall's Cloudflare ranges together with
   [`cloudflare-ips.conf`](../deploy/proxy/conf.d/cloudflare-ips.conf).
2. Install `deploy/compose.yaml` and `deploy/proxy/` from a reviewed commit into
   `/srv/selvage`. Copy `deploy/.env.example` to `.env` once, edit it for this
   instance, and make `.env` mode `0600`; never overwrite it during an upgrade.
3. Install the origin certificate in `tls/origin.pem` (`root:root`, `0644`) and
   key in `tls/origin.key` (`root:101`, `0640`), inside `tls/` (`root:root`,
   `0750`). Certificates and keys are instance material, not repository files.
4. Install `deploy/selvage-update.service` and `deploy/selvage-update.timer` in
   `/etc/systemd/system/`, then run `systemctl daemon-reload`. Bring the shape
   up from `/srv/selvage` with `docker compose up -d --build proxy`; enable the
   timer only after checking the configured images and origin responses.
5. Create the unprivileged `deployci` account. Install `deploy/deploy.py` as
   `/usr/local/sbin/selvage-deploy` and `deploy/install_shape.py` as
   `/usr/local/sbin/selvage-install-shape`, both `root:root`, `0755`. Validate
   the corresponding `deployci.sudoers` and `installci.sudoers` with `visudo -c`
   before installing them in `/etc/sudoers.d/` as `root:root`, `0440`.

The two sudoers commands must retain the trailing `""`: it permits **no
arguments**. Omitting that argument constraint permits arbitrary arguments.
Read the result with `sudo -l -U deployci`. Do not grant CI Docker-group
membership, blanket sudo, or permission to replace these scripts or sudoers
files. Changing either privileged program, the systemd units, `.env` settings
other than image references, or TLS material remains administrator work.

The image deployer accepts only tags of the two allowed GHCR images on stdin.
The shape installer accepts only `compose.yaml` and `proxy/` from a tar stream;
it rejects traversal, absolute paths, links, and special files. Shape access is
still powerful: a reviewed Compose file controls mounts, ports, and capabilities.
The installer does not delete obsolete proxy files; removed files need an
administrator's cleanup before rebuilding the front.

Instance-specific `compose.override.yaml` and notice files are not installed by
CI. A notice mounted into the front must survive shape upgrades. In particular,
nginx `sub_filter` directives at a more specific location replace the inherited
list rather than appending to it; `nginx -t` alone cannot prove a notice renders.

## CI access

[deploy-prod.yml](../.github/workflows/deploy-prod.yml) is dispatched at `main`.
It has **no environment approval gate**. Tailscale workload identity federation
matches the GitHub OIDC subject exactly:

```text
repo:selvage-protocol@328844633/reference_server@1368869493:ref:refs/heads/main
```

The numeric IDs are part of the organization's immutable subject form. Adding
an `environment:` to this job changes the subject and breaks the exchange.
Repository secrets `TS_OAUTH_CLIENT_ID` and `TS_AUDIENCE` name the credential
pair; their values belong in the credential console and GitHub, not this guide.
The ephemeral runner receives `tag:ci`. Tailnet policy must restrict it to SSH on
`tag:selvaged-prod`, with the SSH rule permitting only `deployci`. Tailscale SSH
cannot constrain the command: the host's two argument-free sudo rules are the
command boundary. Tailnet Lock requires a separate authentication design if
enabled; do not silently replace federation with a broad reusable key.

To rotate the pair, create a replacement identity with the same constrained
subject, update both repository secrets, verify a deploy, then remove the old
identity. An exchange refusal needs the actual claims and credential inspected:
a missing identity and a rejected subject are different failures. Never loosen
the subject or host privileges to make a retry pass.

## Deploy and verify

From any checkout, explicitly name this repository:

```sh
gh workflow run deploy-prod.yml -R selvage-protocol/reference_server --ref main \
  -f server_version=<version>
gh workflow run deploy-prod.yml -R selvage-protocol/reference_server --ref main \
  -f server_version=<version> -f install_shape=true
```

An empty image input leaves that line unchanged; `latest` unpins it. The server
release changes its own `SELVAGED_IMAGE` line. The page repository normally
changes `SELVAGE_WEB_IMAGE` independently. The deploy accepts tags, not digests:
a version tag is a deliberate pin but is still mutable by a registry writer.

With `install_shape=true`, CI streams the tracked pair from the run's `main`
ref, installs it under the update lock, and reads installed file hashes back.
Matching files are a no-op; a Compose-only change runs `up -d`; a proxy change
rebuilds the front. To install the same reviewed shape by hand from this repo:

```sh
git archive --format=tar <sha>:deploy compose.yaml proxy \
  | ssh deployci@selvage-protocol-prod \
      '/usr/bin/sudo /usr/local/sbin/selvage-install-shape'
```

The image deploy holds `/srv/selvage/.update.lock`, stages `.env.deploy`, and
writes `.env` only after convergence, keeping `.env.prev`. A failed deploy can
already have changed running containers even though `.env` is unchanged; inspect
both, then reconcile with the intended references. A healthy origin is asserted
through the front over SSH: `/meta` must report the requested server version
when one was specified, and `/` must answer successfully. Read current state:

```sh
ssh selvage@selvage-protocol-prod 'cd /srv/selvage && docker compose ps && docker compose images'
ssh selvage@selvage-protocol-prod \
  "curl -sk -H 'Host: selvage-demo.dontblameme.dev' https://127.0.0.1/meta"
```

The public edge may challenge a datacenter client. Verification reports a
challenge or unreadable public response; it does not bypass bot controls or
claim that the public path passed. A readable public version different from the
requested version fails the deploy. `latest` has no expected version to compare.
A page answering 200 is **not proof of its build identity**. Neither these reads
nor local proxy tests verify the separately hosted site's deployed response
headers or their limits. Read those from the deployed response before claiming
them. Automated checks do not establish human acceptance.

## Timer, recovery, and downtime

The update timer pulls configured tags every five minutes under the same lock.
A line left at `latest` follows registry updates without another approval.
Inspect with `systemctl list-timers selvage-update.timer` and
`journalctl -u selvage-update.service -n 50`.

For manual operations, run Compose as the instance operator in `/srv/selvage`,
without a `-f` override, so it reads the same `.env` and optional override as the
timer. **Stop the timer before stopping services** or its next tick undoes the
stop. To keep it stopped across boots, disable it as well.

```sh
sudo systemctl stop selvage-update.timer
cd /srv/selvage
docker compose down
# Restore the reviewed shape and intended image references before restarting.
docker compose pull --ignore-buildable
docker compose up -d --build proxy
sudo systemctl start selvage-update.timer
```

Rollback is an explicit pin to a known previous image tag followed by pull and
up. After a successful deploy, `.env.prev` is the previous environment; inspect
it before copying it back. Restore older proxy files from a reviewed commit and
rebuild for a front rollback. These are recovery procedures, not a claim that a
live rollback has been exercised.

Replacing `selvaged` ends every room: all state is in memory, with no drain or
restart reclaim. A front replacement drops WebSockets and requires clients to
reconnect; it does not itself restore lost server state. Coordinate disruptive
changes with users rather than treating a green deploy as a seamless upgrade.

The repository's [publishing guide](publishing.md) covers release/image recovery.
