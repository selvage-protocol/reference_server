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
ARG TARGETARCH=amd64
ARG RUNTIME=scratch
ARG VERSION=dev
ARG REVISION=unknown

# One cross-toolchain image per target, always run natively on the build host:
# each stage cross-compiles its target triple, so building arm64 needs no
# QEMU (only *running* the arm64 image does). The `-amd64` suffix is the
# image's host architecture (the CI runners are x86_64), not the target.
# These tags float with upstream stable (no versioned tags are published);
# the build itself is pinned by `--locked`.
FROM --platform=$BUILDPLATFORM messense/rust-musl-cross:x86_64-musl-amd64 AS builder-amd64
FROM --platform=$BUILDPLATFORM messense/rust-musl-cross:aarch64-musl-amd64 AS builder-arm64
FROM builder-${TARGETARCH} AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN if [ "$TARGETARCH" = "arm64" ]; then echo aarch64; else echo x86_64; fi \
        > /tmp/musl-arch \
    && cargo build --locked --release -p selvaged \
        --target "$(cat /tmp/musl-arch)-unknown-linux-musl" \
    && cp "target/$(cat /tmp/musl-arch)-unknown-linux-musl/release/selvaged" \
        /selvaged \
    && /selvaged --version

# `scratch` takes no tag and `runtime-*` is an internal stage, so DL3006
# (always tag the image) is unactionable here by design. hadolint
# directives cannot go on these FROM lines themselves: Docker parses
# trailing text as extra arguments, so they would break the build.
FROM scratch AS runtime-scratch
COPY --from=builder /selvaged /selvaged
COPY crates/selvaged/LICENSE /LICENSE
USER 65532
EXPOSE 8080
ENTRYPOINT ["/selvaged"]
CMD ["--listen", "0.0.0.0:8080"]

FROM gcr.io/distroless/static:nonroot AS runtime-distroless
COPY --from=builder /selvaged /selvaged
COPY crates/selvaged/LICENSE /LICENSE
EXPOSE 8080
ENTRYPOINT ["/selvaged"]
CMD ["--listen", "0.0.0.0:8080"]

FROM runtime-${RUNTIME} AS final
# The FSL-1.1-MIT licence travels inside the image (see /LICENSE) and in its
# annotations. Pushing this image anywhere is redistribution of the binary:
# review the Competing Use scope before publishing — see packaging/README.md.
LABEL org.opencontainers.image.title="selvaged" \
      org.opencontainers.image.description="Memory-only reference server for the Selvage Session Protocol" \
      org.opencontainers.image.source="https://github.com/selvage-protocol/reference_server" \
      org.opencontainers.image.licenses="FSL-1.1-MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
