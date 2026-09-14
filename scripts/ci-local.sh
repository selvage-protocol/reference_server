#!/usr/bin/env bash
#
# Runs the steps of .github/workflows/ci.yml on this machine, without containers (this host
# has no Docker or Podman, so `act` cannot run here).
#
#   scripts/ci-local.sh checks    # the `checks` job: format, clippy, tests, the package, licences, eval, lint
#   scripts/ci-local.sh nightly   # coverage, the rest of cargo-deny and cargo-audit (slow)
#   scripts/ci-local.sh lint      # actionlint over the workflow files, on its own
#   scripts/ci-local.sh all       # everything the `checks` job runs (nightly is opt-in: it is slow)
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
  say "checks: lint the workflows"
  nix develop . -c actionlint
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

case "${1:-all}" in
  checks) job_checks ;;
  nightly) job_nightly ;;
  lint) job_lint ;;
  all) job_checks ;;
  *)
    printf 'usage: %s [checks|nightly|lint|all]\n' "$0" >&2
    exit 2
    ;;
esac
