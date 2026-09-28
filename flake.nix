{
  description = "Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    crane,
    flake-utils,
    rust-overlay,
    git-hooks,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [(import rust-overlay)];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.minimal.override {
          extensions = [
            "rust-src"
            "rustfmt"
            "clippy"
            "rust-analyzer"
          ];
        };

        craneLib =
          (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
          pname = binName;

          # `cleanCargoSource` copies the Cargo workspace and nothing else, so without this the
          # sandbox cannot see `vectors/` and `crates/harness/tests/vectors.rs`,
          # `peer_vectors.rs` and `decisions.rs` panic on the corpus instead of running. It
          # belongs on the shared args
          # rather than on one check: `nextest` and `tarpaulin` each carried their own copy,
          # `packages.default` — which is `checks.build` — carried none, and that is why it
          # failed to build from the day the vectors were vendored.
          #
          # `doCheck = false` on the package was the alternative, leaving `checks.nextest` as
          # the only test gate. It is cheaper — `nix run` would not wait on the suite, and this
          # line invalidates `cargoArtifacts` — but it answers a missing input by not testing.
          SELVAGE_VECTORS = ./vectors;
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        package = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
          }
        );

        # The container image, assembled without a Docker daemon: the runners
        # that build it cannot execute buildx builds, so CI smokes this
        # instead of the Dockerfile. One architecture per builder — the
        # expression is arch-agnostic and the workflow builds it natively on
        # x86_64 and aarch64 runners, no cross-compilation anywhere.
        # `copyToRoot` pulls the binary's whole closure (glibc and friends)
        # along, so this image is nix-idiomatic rather than static-minimal.
        # The Dockerfile stays the portable static variant for machines with
        # a working Docker; both agree on entrypoint, port, user, licence
        # label and the <version>-<sha> tag scheme.
        image = pkgs.dockerTools.buildImage {
          name = "ghcr.io/selvage-protocol/selvaged";
          tag = workspaceVersion;
          copyToRoot = [package];
          extraCommands = ''
            cp ${./crates/selvaged/LICENSE} LICENSE
          '';
          config = {
            Entrypoint = ["/bin/selvaged"];
            Cmd = ["--listen" "0.0.0.0:8080"];
            ExposedPorts = {"8080/tcp" = {};};
            User = "65532";
            Labels = {
              "org.opencontainers.image.title" = "selvaged";
              "org.opencontainers.image.description" = "Memory-only reference server for the Selvage Session Protocol";
              "org.opencontainers.image.source" = "https://github.com/selvage-protocol/reference_server";
              "org.opencontainers.image.licenses" = "FSL-1.1-MIT";
              "org.opencontainers.image.version" = workspaceVersion;
            };
          };
        };

        mkHook = name: entry: {
          enable = true;
          inherit name entry;
          language = "system";
          pass_filenames = false;
        };

        hooks = git-hooks.lib.${system}.run {
          src = ./.;

          hooks = {
            alejandra.enable = true;
            deadnix.enable = true;
            flake-checker.enable = true;
            gitlint.enable = true;
            check-merge-conflicts.enable = true;
            forbid-new-submodules.enable = true;
            check-json.enable = true;
            lychee = {
              enable = true;
            };
            comrak = {
              enable = true;
              # The README and the `docs/` tree beside it are prose this project
              # maintains by hand; a formatter must not rewrite them.
              excludes = ["^(README\\.md|docs/)"];
            };
            ripsecrets.enable = true;
            typos.enable = true;
            check-toml.enable = true;
            check-yaml.enable = true;
            check-executables-have-shebangs.enable = true;
            check-shebang-scripts-are-executable.enable = true;
            check-added-large-files = {
              enable = true;
              # The wire vectors are vendored test data, whose sizes are the
              # specification's business rather than this guard's: the guard is
              # for source and assets, not for the corpus.
              excludes = ["^vectors/"];
            };
            check-symlinks.enable = true;
            trim-trailing-whitespace = {
              enable = true;
              # Same reason as `comrak` above: the README and `docs/` are
              # hand-maintained prose.
              excludes = ["^(README\\.md|docs/)"];
            };
            shellcheck.enable = true;

            woodpecker-cli-lint = {
              enable = true;
              files = "\\.woodpecker/";
            };

            rustfmt = mkHook "rustfmt" "nix build .#checks.${system}.fmt --no-link --print-build-logs";
            cargo-check = mkHook "cargo check" "nix build .#checks.${system}.clippy --no-link --print-build-logs";
            clippy = mkHook "clippy" "nix build .#checks.${system}.clippy --no-link --print-build-logs";
            audit = mkHook "audit" "${pkgs.cargo-audit}/bin/cargo-audit audit --file Cargo.lock";

            deny =
              mkHook
              "deny"
              "${pkgs.cargo-deny}/bin/cargo-deny --manifest-path Cargo.toml check";
            tarpaulin = mkHook "tarpaulin" "nix build .#checks.${system}.tarpaulin --no-link --print-build-logs";

            cargo-nextest =
              mkHook
              "cargo nextest"
              "nix build .#checks.${system}.nextest --no-link --print-build-logs";
          };
        };
        binCargoPath = ./crates/selvaged/Cargo.toml;
        cargoToml = builtins.fromTOML (builtins.readFile binCargoPath);
        binName = cargoToml.package.name;
        workspaceVersion =
          (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in {
        formatter = pkgs.alejandra;
        packages.default = package;
        packages.image = image;
        # skopeo inspects the image in the smoke without a daemon. A package
        # (run by direct path after `nix build`), not a devShell command:
        # `nix develop` is the interactive environment, and the smoke stays
        # hermetic.
        packages.skopeo = pkgs.skopeo;

        apps.default = {
          type = "app";
          program = "${package}/bin/${binName}";
        };

        checks = {
          pre-commit = hooks;

          build = package;

          clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-features --all-targets -- -D warnings";
            }
          );

          nextest = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              partitions = 1;
              partitionType = "count";
              # `cargo nextest`'s default target selection leaves examples out, and clippy's
              # `--all-targets` does not stand in for it: cargo checks an example as the binary
              # it ships, so the `#[cfg(test)]` module one carries — `interop_peer`'s command
              # line — was built by nothing. It did not compile, and no suite could say so.
              cargoNextestExtraArgs = "--all-targets";
              # The vectors sit outside this Cargo workspace, so the sandbox — which receives
              # only the workspace — is handed them explicitly.
              SELVAGE_VECTORS = ./vectors;
            }
          );

          tarpaulin = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit src cargoArtifacts;
              SELVAGE_VECTORS = ./vectors;
              pname = "${binName}-tarpaulin";
              buildPhaseCargoCommand = "cargo tarpaulin --engine llvm --fail-under 80";
              installPhase = "mkdir -p $out";
              nativeBuildInputs = [pkgs.cargo-tarpaulin];
            }
          );

          fmt = craneLib.cargoFmt {
            inherit src;
          };

          # The guard around `deploy/deploy.py`, which is the whole of the
          # public demo's privilege model: `/usr/local/sbin/selvage-deploy` is the
          # only root command the CI user on that box may run. Nothing else in this
          # repository can see what its request grammar refuses, or that the compose
          # file and the update unit agree with it.
          prod-deploy =
            pkgs.runCommand "prod-deploy-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./deploy}
              python3 -B test_deploy.py
              touch $out
            '';

          # The guard around `deploy/install_shape.py`, the second root command
          # the CI user on that box may run: the box's shape — `compose.yaml` and
          # `proxy/` — arrives as a tar stream on stdin, and every other name,
          # absolute or traversing path, link, device and fifo is refused with the
          # box untouched. Nothing else in this repository can see what it refuses,
          # or that a step is resolved inside the descriptor the step before it
          # found rather than checked and then used.
          prod-shape =
            pkgs.runCommand "prod-shape-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./deploy}
              INSTALL_SHAPE_WORKDIR="''${TMPDIR:-/build}/install-shape" python3 -B test_install_shape.py
              touch $out
            '';

          # The public demo's front, under its real configuration and a real nginx:
          # whether a source is refused at all, whether one metered endpoint can
          # spend another's budget, whether a missing path is answered with this
          # instance's own not-found page, and whether a request naming another
          # host is closed rather than served. All four are questions about a
          # running proxy, and the second is the reason the front's zones are one
          # per location; nothing else in this repository can see any of them.
          prod-front =
            pkgs.runCommand "prod-front-test" {
              nativeBuildInputs = [pkgs.python3 pkgs.nginx pkgs.curl];
            } ''
              cd ${./deploy}
              FRONT_LIMITS_WORKDIR="''${TMPDIR:-/build}/front-limits" python3 -B test_front_limits.py
              CHECK_FRONT_WORKDIR="''${TMPDIR:-/build}/check-front" bash check-front.sh
              touch $out
            '';

          # The command lines the tracked compose shapes give the server, run
          # against the binary that runs them: a value it refuses is a container
          # that exits 2 and restart-loops, which takes the front beside it down
          # with it.
          deploy-args =
            pkgs.runCommand "deploy-args-test" {
              nativeBuildInputs = [pkgs.python3 package];
            } ''
              cd ${./deploy}
              python3 -B test_compose_args.py
              touch $out
            '';

          # The guard around `scripts/bump-version.sh`, the command this
          # repository's release workflow runs before it commits a version: a bump
          # word in, the next version on the last line of stdout, and the two files
          # that carry the version moving together — with nothing written at all for
          # a word, an argument or a manifest it cannot compute a version from. It
          # reaches cargo through `PATH`, so this runs it against a stand-in and
          # needs no toolchain.
          bump-version =
            pkgs.runCommand "bump-version-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./scripts}
              python3 -B test_bump_version.py
              touch $out
            '';

          # The deploy workflow's verification: what it asserts (the origin,
          # read on the box through the front over the SSH path the deploy itself
          # used) and what it only reports (the public URL, behind an edge that
          # serves a managed challenge to a programmatic client on a datacenter
          # address). The second half turned a healthy deploy red once, and the
          # first is the half that has to fail when the box serves the wrong
          # version; nothing else in this repository can see either.
          deploy-verify =
            pkgs.runCommand "deploy-verify-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./scripts}
              python3 -B test_verify_deploy.py
              touch $out
            '';

          # The watcher the release's last step runs (`scripts/wait_for_deploy.py`):
          # `gh workflow run` prints no run id, so the run to wait on is the newest
          # one of the workflow that is not the newest seen before the dispatch, and
          # the release fails unless that run concludes `success`. The suite drives
          # the poll loops with injected reads and runs the `gh`-facing edges against
          # a stand-in on `PATH`, so it needs no network, no token and no real run.
          deploy-wait =
            pkgs.runCommand "deploy-wait-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./scripts}
              python3 -B test_wait_for_deploy.py
              touch $out
            '';

          # The guard around a workflow's `dry_run` input. `release.yml` printed a
          # plan promising that nothing was resolved, written or dispatched and then
          # resolved, wrote, committed, pushed and dispatched everything, because the
          # plan step was the only step carrying a condition. `actionlint` lints that
          # file clean — every step it does have is a valid expression, and the defect
          # is the steps that have none — so this reads the workflows back and refuses
          # one where a step after the plan can still run on a dry run. The suite
          # covers the condition spellings and the residual it does not read; the
          # second command is this repository's own workflows.
          dry-run-gating =
            pkgs.runCommand "dry-run-gating-test" {
              nativeBuildInputs = [
                (pkgs.python3.withPackages (ps: [ps.pyyaml]))
              ];
            } ''
              cd ${./scripts}
              python3 -B test_check_dry_run_gating.py
              python3 -B check_dry_run_gating.py ${./.github/workflows}
              touch $out
            '';
        };

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};
          inputsFrom = [package];

          packages = with pkgs;
            [
              rustToolchain
              pkg-config
              cargo-nextest
              cargo-watch
              cargo-audit
              cargo-deny
              cargo-tarpaulin
              bacon
              nodejs_22
              actionlint
            ]
            ++ hooks.enabledPackages;

          RUST_BACKTRACE = "1";

          inherit (hooks) shellHook;
        };
      }
    );
}
