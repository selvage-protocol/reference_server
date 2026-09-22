#!/usr/bin/env bash
#
# Runs the steps of .github/workflows/ci.yml on this machine, without containers (this host
# has no Docker or Podman, so `act` cannot run here).
#
#   scripts/ci-local.sh checks    # the `checks` job: format, clippy, tests, the package, licences, eval, the TLS front, the two
#                                 # demos' deploy and front checks, the deploy workflow's verification, lint, typos
#   scripts/ci-local.sh nightly   # coverage, the rest of cargo-deny and cargo-audit (slow)
#   scripts/ci-local.sh lint      # actionlint over the workflow files, on its own
#   scripts/ci-local.sh image     # the `image` workflow's smoke: nix-built image,
#                                 # skopeo manifest/config checks, version
#                                 # assertions — no Docker, runs anywhere nix does
#   scripts/ci-local.sh container # the `image` workflow's container job: docker
#                                 # build/run hardened plus a room join (needs Docker
#                                 # and the compose plugin)
#   scripts/ci-local.sh all       # everything the `checks` job runs (nightly is opt-in: it is slow)
#
# The `image` workflow's `publish-rehearsal` and `publish` jobs (multi-arch buildx) have no
# step here: this host has no Docker, let alone buildx, so they are read from the run and
# from `scripts/release-tags.sh`, `scripts/assert-image-version.sh` and
# `scripts/assert-multiarch-layers.py`, which those jobs and this script's container job share.
# The last of those needs only network access to a registry, not Docker: run it by hand against
# any published tag, e.g. `scripts/assert-multiarch-layers.py ghcr.io/selvage-protocol/selvaged:0.1.2`.
#
# Keep this in step with the workflow — it runs the same commands, so that a red job is found
# here rather than on a runner. `lint` catches unknown actions, bad expressions and shell
# mistakes statically.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

# `/tmp` is a RAM-backed tmpfs on some hosts, and building there has taken a machine down
# before; keep every artefact inside the checkout and cap parallelism.
export TMPDIR="$repo_root/.tmp"
mkdir -p "$TMPDIR"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
export NIX_BUILD_CORES=${NIX_BUILD_CORES:-2}

say() { printf '\n=== %s ===\n' "$*"; }

job_checks() {
  say "checks: format"
  nix build .#checks.x86_64-linux.fmt --no-link --print-build-logs
  say "checks: clippy"
  nix build .#checks.x86_64-linux.clippy --no-link --print-build-logs
  say "checks: tests"
  nix build .#checks.x86_64-linux.nextest --no-link --print-build-logs
  # `packages.default` is what `nix run` hands back, and until this step existed nothing built
  # it: `nix flake check --no-build` below only *evaluates* it, so it could be — and was —
  # broken without any job noticing.
  say "checks: the default package"
  nix build .#default --no-link --print-build-logs
  say "checks: licences"
  nix develop . -c cargo deny check licenses
  say "checks: evaluate every check"
  nix flake check --no-build .
  # The TLS front's idle logic, stdlib Python and no Pi: the one check that is not a
  # Cargo target.
  say "checks: the TLS front's idle logic"
  nix build .#checks.x86_64-linux.tls-proxy --no-link --print-build-logs
  # The guard around the public demo's deploy script, which is the whole of that
  # box's privilege model: one request grammar, and a shape the deploy verifies
  # rather than writes.
  say "checks: the public demo's deploy guard"
  nix build .#checks.x86_64-linux.prod-deploy --no-link --print-build-logs
  # The public demo's front under a real nginx: that a source is refused, and that
  # one metered endpoint cannot spend another's budget. Both are about a running
  # proxy, so neither can be read out of the configuration.
  say "checks: the public demo's front limits"
  nix build .#checks.x86_64-linux.prod-front --no-link --print-build-logs
  # The deploy workflow's verification: what it asserts (the origin, on the box
  # through the front) and what it only reports (the public URL, behind an edge
  # that answers a datacenter client with a challenge).
  say "checks: the deploy workflow's verification"
  nix build .#checks.x86_64-linux.deploy-verify --no-link --print-build-logs
  say "checks: lint the workflows"
  nix develop . -c actionlint
  # The same check the pre-commit hook runs: a commit made outside the dev shell cannot
  # run the hooks, so a spelling error could only be caught on the pull request.
  say "checks: typos"
  nix develop . -c typos
}

job_nightly() {
  say "nightly: coverage"
  nix build .#checks.x86_64-linux.tarpaulin --no-link --print-build-logs
  say "nightly: cargo-deny"
  nix develop . -c cargo deny check
  say "nightly: cargo-audit"
  nix develop . -c cargo audit
}

job_lint() {
  say "lint: actionlint over the workflows"
  nix develop . -c actionlint
}

job_image() {
  say "image: nix-built image smoke without publishing"
  scripts/image-smoke.sh
}

job_container() {
  say "container: docker build, docker run, a room joined in the container"
  scripts/container-smoke.sh
}

case "${1:-all}" in
  checks) job_checks ;;
  nightly) job_nightly ;;
  lint) job_lint ;;
  image) job_image ;;
  container) job_container ;;
  all) job_checks ;;
  *)
    printf 'usage: %s [checks|nightly|lint|all|image|container]\n' "$0" >&2
    exit 2
    ;;
esac
