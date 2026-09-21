# syntax=docker/dockerfile:1
#
# The public demo's TLS terminator: nginx on the same base image the page image
# runs, configured by the files beside this one and by nothing else.
#
# The image exists so that the proxy's only mount is its certificate. nginx's
# configuration could be bind-mounted instead, but a mount is a view of the host
# the container did not have to be given, and this deployment's stance is that
# each container gets the fewest of those it can work with — `packaging/prod/
# compose.yaml` names the one this service has.
#
# The base is pinned by the index digest the tag resolved to, not by the tag:
# a tag is a name someone else can repoint, and this container terminates TLS
# for the public origin. The digest is the multi-architecture index, so
# `TARGETARCH` is still resolved at pull time. To move it, resolve the tag again
# (the flow in `reference_server/scripts/assert-multiarch-layers.py`) and change
# this line in a pull request.
#
# The runtime is `nginx-unprivileged`: uid 101, listening on 8080, so nothing
# here needs a capability or a privileged port, and `cap_drop: [ALL]` costs the
# container nothing. Its writable paths — the pid file and the five temp
# directories — are moved onto `/dev/shm` by `nginx.conf`, the tmpfs the
# container runtime mounts itself, which is what lets it run under `read_only`
# with no writable mount at all.
FROM nginxinc/nginx-unprivileged:1.30-alpine-slim@sha256:6917744e63b3c4bde796d9d4f55e572dd77d1de860228616cad7301f2e5d1930

LABEL org.opencontainers.image.title="selvage-prod-proxy" \
      org.opencontainers.image.description="TLS terminator and router for the public Selvage demo" \
      org.opencontainers.image.source="https://github.com/selvage-protocol/reference_server" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

# The base image's own `conf.d/default.conf` answers every request with the
# nginx welcome page on 8080. Replaced wholesale rather than overlaid: a server
# block that is still listening when this one fails to start is a front that
# looks healthy and is not.
RUN rm -f /etc/nginx/conf.d/*.conf

COPY --chown=101:101 nginx.conf /etc/nginx/nginx.conf
COPY --chown=101:101 conf.d/ /etc/nginx/conf.d/

# Straight to nginx, past the base image's `/docker-entrypoint.sh`. Those scripts
# exist to rewrite the configuration at startup — enabling the IPv6 listen,
# substituting environment variables into templates, sizing the worker count —
# and this image's configuration is a reviewed file, not a template: what runs
# has to be what is in this repository, and a script that edits it at boot is a
# second source of truth that only appears on the machine. Nothing here needs
# what they add; `30-tune-worker-processes.sh` in particular would override the
# `worker_processes 1` that `nginx.conf` reasons about for this box.
ENTRYPOINT ["/usr/sbin/nginx", "-g", "daemon off;"]

EXPOSE 8080
