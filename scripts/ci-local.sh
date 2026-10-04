#!/usr/bin/env bash
#
# Runs the steps of .github/workflows/ci.yml on this machine, without containers (this host
# has no Docker or Podman, so `act` cannot run here).
#
#   scripts/ci-local.sh checks    # the `checks` job: format, clippy, tests, the package, licences, eval, the public
#                                 # demo's deploy guard, front and command line, the version
#                                 # bump, the deploy workflow's verification,
#                                 # the release's wait for its deploy, the release workflow's
#                                 # dry_run gating, lint, typos, links
#   scripts/ci-local.sh nightly   # coverage, the rest of cargo-deny and cargo-audit (slow)
#   scripts/ci-local.sh lint      # actionlint over the workflow files, on its own
#   scripts/ci-local.sh links     # lychee over the README and the docs tree
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
# any published tag.
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

# What a `nix build` here reads is the tracked tree at its working-tree content: a file that is
# new and untracked is invisible to it, while a modified or deleted tracked file is read as it
# stands. Either way the run is not the run CI would do — CI checks out the committed ref — so
# the jobs that build refuse when the tree differs from HEAD, and say so. Commit, not just
# stage: staged-but-uncommitted is visible to the local build and absent from CI. One thing
# this guard is for: an untracked test file that would run the wire suite is invisible to the
# build, which then reports a green run of a smaller suite. The steps that run
# in the dev shell (`cargo deny`, `actionlint`, `typos`) and the `container` job read the
# working tree directly, so they carry no guard.
inputs_clean() {
  local dirty
  dirty=$(git status --porcelain)
  if [[ -n $dirty ]]; then
    printf 'refusing: the tree differs from HEAD, so this is not the run CI would do.\n' >&2
    printf 'commit, or stash, then re-run:\n' >&2
    printf '%s\n' "$dirty" >&2
    return 1
  fi
}

job_checks() {
  inputs_clean
  say "checks: format"
  nix build .#checks.x86_64-linux.fmt --no-link --print-build-logs
  say "checks: clippy"
  nix build .#checks.x86_64-linux.clippy --no-link --print-build-logs
  say "checks: tests"
  nix build .#checks.x86_64-linux.nextest --no-link --print-build-logs
  # `packages.default` is what `nix run` hands back, and nothing else here builds it:
  # `nix flake check --no-build` below only *evaluates* it, so it can be broken without
  # any job noticing.
  say "checks: the default package"
  nix build .#default --no-link --print-build-logs
  say "checks: licences"
  nix develop . -c cargo deny check licenses
  say "checks: evaluate every check"
  nix flake check --no-build .
  # The guard around the public demo's deploy script, which is the whole of that
  # box's privilege model: one request grammar, and a shape the deploy verifies
  # rather than writes.
  say "checks: the public demo's deploy guard"
  nix build .#checks.x86_64-linux.prod-deploy --no-link --print-build-logs
  # The other root command the CI user on that box may run: the box's shape —
  # `compose.yaml` and `proxy/` — arrives as a tar stream, and every other name,
  # link, device and traversing path is refused with the box untouched.
  say "checks: the public demo's shape installer"
  nix build .#checks.x86_64-linux.prod-shape --no-link --print-build-logs
  # The public demo's front under a real nginx, in both halves: that a source is
  # refused and that one metered endpoint cannot spend another's budget
  # (`test_front_limits.py`), and that a missing path is answered with this
  # instance's own not-found page and a request naming another host is closed
  # (`check-front.sh`). Every one is about a running proxy, so none can be read
  # out of the configuration.
  say "checks: the public demo's front"
  nix build .#checks.x86_64-linux.prod-front --no-link --print-build-logs
  # The command lines the tracked compose shapes give the server, run against the
  # binary: a value it refuses is a container that exits 2 and restart-loops.
  say "checks: the deployed command line"
  nix build .#checks.x86_64-linux.deploy-args --no-link --print-build-logs
  # The version bump this repository's release workflow runs before it commits:
  # the next version from a bump word, on the last line of stdout, and nothing
  # written for a word or a manifest it cannot compute from.
  say "checks: the version bump"
  nix build .#checks.x86_64-linux.bump-version --no-link --print-build-logs
  # The deploy workflow's verification: what it asserts (the origin, on the box
  # through the front) and what it only reports (the public URL, behind an edge
  # that answers a datacenter client with a challenge).
  say "checks: the deploy workflow's verification"
  nix build .#checks.x86_64-linux.deploy-verify --no-link --print-build-logs
  # The watcher the release's last step runs: `gh workflow run` prints no run id,
  # so the run to wait on is the newest one not seen before the dispatch, and the
  # release fails unless it concludes `success`. The suite drives the polls with
  # injected reads and a stand-in `gh` on `PATH`.
  say "checks: the release waits for its deploy"
  nix build .#checks.x86_64-linux.deploy-wait --no-link --print-build-logs
  # The guard around a workflow's `dry_run` input: a plan step prints what a real
  # run would do, and every step after it has to be excluded from a dry run.
  # `actionlint` lints those workflows clean, so this reads the workflows back and refuses
  # one where a step after the plan can still run on a dry run.
  say "checks: the release workflow's dry_run gating"
  nix build .#checks.x86_64-linux.dry-run-gating --no-link --print-build-logs
  say "checks: lint the workflows"
  nix develop . -c actionlint
  # The same check the pre-commit hook runs: a commit made outside the dev shell cannot
  # run the hooks, so a spelling error could only be caught on the pull request.
  say "checks: typos"
  nix develop . -c typos
  job_links
}

job_nightly() {
  inputs_clean
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

job_links() {
  say "links: lychee over the README and the docs tree"
  # The scope is this repository's reader-facing front matter, not every Markdown
  # file a checkout holds, and it is the same scope and the same arguments as the
  # workflow's `links` step. The runner installs lychee 0.24.2 from the pinned
  # release; the dev shell carries the same version through the git-hooks tool set,
  # so the two runs are the same check.
  nix develop . -c lychee --config lychee.toml --no-progress README.md docs
}

job_image() {
  inputs_clean
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
  links) job_links ;;
  image) job_image ;;
  container) job_container ;;
  all) job_checks ;;
  *)
    printf 'usage: %s [checks|nightly|lint|links|all|image|container]\n' "$0" >&2
    exit 2
    ;;
esac
