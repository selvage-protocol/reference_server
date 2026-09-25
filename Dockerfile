# syntax=docker/dockerfile:1
#
# selvaged — multi-arch static image for strangers and VPS self-hosters,
# never for the Pi (which runs the native binary under the user unit in
# packaging/systemd/).
#
# Primary shape: a musl-static binary on scratch. The lockfile carries no TLS
# C shims (no openssl/ring/aws-lc-sys — only libc/mio as OS shims), so the
# static build needs no extra C libraries.
#
# Fallback shape: the same static binary on distroless (`RUNTIME=distroless`).
# If a future dependency ever breaks the musl build, compile a glibc binary
# instead and keep the distroless runtime — the stage below already accepts
# any binary at /selvaged. See packaging/README.md.
#
# The image also serves the browser page: a `page` stage builds the
# `web_client` bundle at the pinned revision below and copies it to /page,
# which the runtime's default command hands to `--serve-page`. Serving a page
# needs no mount; the override is a mount (see compose.yaml).
# No default: buildx supplies this per platform automatically, and giving it
# one here — even just for a plain, non-buildx `docker build .` fallback —
# shadows that per-platform value for every later `${TARGETARCH}`, including
# the `FROM builder-${TARGETARCH}` line below, which is exactly how both
# legs of 0.1.0 and 0.1.1 built from the amd64 stage regardless of platform.
# A plain `docker build .` still gets a correct value: Docker populates it
# from the build machine's own architecture when there is no buildx platform
# to draw it from.
ARG TARGETARCH
ARG RUNTIME=scratch
ARG VERSION=dev
ARG REVISION=unknown
# The browser client revision the baked page is built from: a commit on
# `selvage-protocol/web_client`, as a full SHA (a shallow fetch by revision
# demands one). The page it builds must speak the wire this server seats, so the
# pin names a revision whose bundle names `selvage/2`: a revision that names
# another wire builds a page that cannot join the container beside it, and
# `scripts/container-smoke.sh` asserts that string of the served bundle. The pin
# is a revision of another repository, so it moves when that one releases, and a
# wave that cuts the two together repins it before this image is cut. The image
# records the revision it carries in `com.selvage.page.revision`.
ARG WEB_CLIENT_SHA=75cd9fee4dcce852219be1c4a6f9a91442f14524

# One cross-toolchain image per target, always run natively on the build host:
# each stage cross-compiles its target triple, so building arm64 needs no
# QEMU (only *running* the arm64 image does). The `-amd64` suffix is the
# image's host architecture (the CI runners are x86_64), not the target.
# These tags float with upstream stable (no versioned tags are published);
# the build itself is pinned by `--locked`.
FROM --platform=$BUILDPLATFORM messense/rust-musl-cross:x86_64-musl-amd64 AS builder-amd64
FROM --platform=$BUILDPLATFORM messense/rust-musl-cross:aarch64-musl-amd64 AS builder-arm64
FROM builder-${TARGETARCH} AS builder
# FROM clears every ARG a stage did not ask for, so this stage needs its own
# ARG TARGETARCH to read the value at all; without it $TARGETARCH below is
# empty and the arch check two lines down always takes its else branch,
# regardless of which cross-toolchain stage FROM just selected.
ARG TARGETARCH
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# The binary's ELF header is checked against the architecture this stage was
# chosen for — `e_machine`, byte 18 of a little-endian ELF: 62 is x86-64, 183
# is AArch64. Without it a graph that resolved `builder-${TARGETARCH}` to the
# wrong cross-toolchain would still produce an image, and a runtime assertion
# could still pass, because the runner's binfmt registrations execute either
# architecture's binary. The toolchain image is Ubuntu with binutils.
RUN if [ "$TARGETARCH" = "arm64" ]; then arch=aarch64; machine=183; else arch=x86_64; machine=62; fi \
    && cargo build --locked --release -p selvaged \
        --target "$arch-unknown-linux-musl" \
    && cp "target/$arch-unknown-linux-musl/release/selvaged" /selvaged \
    && [ "$(od -An -tu1 -j18 -N1 /selvaged | tr -d ' ')" = "$machine" ] \
    && /selvaged --version

# The page: the browser client's built `dist/`, cloned at the pinned revision
# and bundled here so a container serves one with no mount. It is built from
# that revision's source rather than from the `dist/` committed there — at
# this pin the two are byte-identical (a clone built with `npm ci && npm run
# build` leaves `git status` clean), so the image carries the reviewed bytes
# and the page cannot fall behind its source. `--platform=$BUILDPLATFORM`: the
# bundle is the same on every architecture, so this stage runs natively on the
# build host instead of once per target under emulation.
# Two things in this base are the stage's own: trixie for ImageMagick 7's
# `magick`, which the client's build shells out to for the sized icons
# (bookworm's imagemagick is 6.x and installs `convert` only), and an explicit
# `ca-certificates`, because the official node images install it and then purge
# it as auto-removable in the same layer — no layer of `node:22-trixie-slim`
# holds an `/etc/ssl` path. Node carries its own trusted roots and works
# without it; git, which fetches the pinned revision over HTTPS here, does not.
FROM --platform=$BUILDPLATFORM node:22-trixie-slim AS page
ARG WEB_CLIENT_SHA
WORKDIR /web_client
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git imagemagick \
    && rm -rf /var/lib/apt/lists/*
RUN git init -q . \
    && git remote add origin https://github.com/selvage-protocol/web_client.git \
    && git fetch --depth 1 origin "$WEB_CLIENT_SHA" \
    && git checkout -q --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "$WEB_CLIENT_SHA" \
    && npm ci --no-audit --no-fund \
    && npm run build

# `scratch` takes no tag and `runtime-*` is an internal stage, so DL3006
# (always tag the image) is unactionable here by design. hadolint
# directives cannot go on these FROM lines themselves: Docker parses
# trailing text as extra arguments, so they would break the build.
FROM scratch AS runtime-scratch
COPY --from=builder /selvaged /selvaged
COPY crates/selvaged/LICENSE /LICENSE
COPY --from=page /web_client/dist /page
USER 65532
EXPOSE 8080
ENTRYPOINT ["/selvaged"]
CMD ["--listen", "0.0.0.0:8080", "--serve-page", "/page"]

FROM gcr.io/distroless/static:nonroot AS runtime-distroless
COPY --from=builder /selvaged /selvaged
COPY crates/selvaged/LICENSE /LICENSE
COPY --from=page /web_client/dist /page
EXPOSE 8080
ENTRYPOINT ["/selvaged"]
CMD ["--listen", "0.0.0.0:8080", "--serve-page", "/page"]

FROM runtime-${RUNTIME} AS final
ARG VERSION
ARG REVISION
ARG WEB_CLIENT_SHA
# The FSL-1.1-MIT licence travels inside the image (see /LICENSE) and in its
# annotations. Pushing this image anywhere is redistribution of the binary;
# the owner accepted GHCR distribution, and re-hosts of the published image,
# as permitted redistribution on 2026-09-19 — see packaging/README.md.
LABEL org.opencontainers.image.title="selvaged" \
      org.opencontainers.image.description="Memory-only reference server for the Selvage Session Protocol, serving the browser client's built page" \
      org.opencontainers.image.source="https://github.com/selvage-protocol/reference_server" \
      org.opencontainers.image.licenses="FSL-1.1-MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      com.selvage.page.revision="${WEB_CLIENT_SHA}"
