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
          # sandbox cannot see `vectors/` and the four anchors-crossing tests in
          # `crates/harness/tests/awareness.rs` panic on the fixture instead of running. It
          # belongs on the shared args rather than on one check: `nextest` and `tarpaulin` each
          # carried their own copy, `packages.default` — which is `checks.build` — carried
          # none, and that is why it failed to build from the day the vectors were vendored.
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
        # label and the <version>-<sha> tag scheme (see packaging/README.md).
        # It additionally bakes the browser page and passes `--serve-page` in
        # its command, which this one does not: the page needs node to build,
        # and this image is the daemon-free shape the smoke can build.
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
              # The README is prose this project maintains by hand; a formatter must not
              # rewrite it.
              excludes = ["^README\\.md$"];
            };
            ripsecrets.enable = true;
            typos.enable = true;
            check-toml.enable = true;
            check-yaml.enable = true;
            check-executables-have-shebangs.enable = true;
            check-shebang-scripts-are-executable.enable = true;
            check-added-large-files = {
              enable = true;
              # The wire vectors are vendored test data, and one is a full-room
              # transcript with every peer seated: megabytes on purpose. The guard
              # is for source and assets, not for the corpus.
              excludes = ["^vectors/"];
            };
            check-symlinks.enable = true;
            trim-trailing-whitespace = {
              enable = true;
              excludes = ["^README\\.md$"];
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

          # The TLS front's idle logic (`packaging/pi-demo/tls-proxy.py`): a quiet half
          # of a tunnel is not a dead one, and nothing else in this repository can see
          # that. Stdlib Python only, so it needs neither the Pi nor a certificate.
          tls-proxy =
            pkgs.runCommand "tls-proxy-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./packaging/pi-demo}
              python3 -B test_tls_proxy.py
              touch $out
            '';

          # The guard around `packaging/prod/deploy.py`, which is the whole of the
          # public demo's privilege model: `/usr/local/sbin/selvage-deploy` is the
          # only root command the CI user on that box may run. Nothing else in this
          # repository can see what its request grammar refuses.
          prod-deploy =
            pkgs.runCommand "prod-deploy-test" {
              nativeBuildInputs = [pkgs.python3];
            } ''
              cd ${./packaging/prod}
              python3 -B test_deploy.py
              touch $out
            '';

          # The public demo's front, under its real configuration and a real nginx:
          # whether a source is refused at all, and whether one metered endpoint can
          # spend another's budget. Both are questions about a running proxy, and the
          # second is the reason the front's zones are one per location; nothing else
          # in this repository can see either.
          prod-front =
            pkgs.runCommand "prod-front-test" {
              nativeBuildInputs = [pkgs.python3 pkgs.nginx];
            } ''
              cd ${./packaging/prod}
              FRONT_LIMITS_WORKDIR="''${TMPDIR:-/build}/front-limits" python3 -B test_front_limits.py
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
