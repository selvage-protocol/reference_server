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
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        package = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
          }
        );

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
            lychee.enable = true;
            comrak.enable = true;
            ripsecrets.enable = true;
            typos.enable = true;
            check-toml.enable = true;
            check-yaml.enable = true;
            check-executables-have-shebangs.enable = true;
            check-shebang-scripts-are-executable.enable = true;
            check-added-large-files.enable = true;
            check-symlinks.enable = true;
            trim-trailing-whitespace.enable = true;
            shellcheck.enable = true;

            woodpecker-cli-lint = {
              enable = true;
              files = "\\.woodpecker/";
            };

            rustfmt = mkHook "rustfmt" "nix build .#checks.${system}.fmt --no-link --print-build-logs";
            cargo-check = mkHook "cargo check" "nix build .#checks.${system}.clippy --no-link --print-build-logs";
            clippy = mkHook "clippy" "nix build .#checks.${system}.clippy --no-link --print-build-logs";
            audit = mkHook "audit" "${pkgs.cargo-audit}/bin/cargo-audit audit";
            deny = mkHook "deny" "${pkgs.cargo-deny}/bin/cargo-deny check";
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
      in {
        formatter = pkgs.alejandra;
        packages.default = package;

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
            }
          );

          tarpaulin = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit src cargoArtifacts;
              pname = "${binName}-tarpaulin";
              buildPhaseCargoCommand = "cargo tarpaulin --fail-under 80";
              installPhase = "mkdir -p $out";
              nativeBuildInputs = [pkgs.cargo-tarpaulin];
            }
          );

          fmt = craneLib.cargoFmt {
            inherit src;
          };
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
            ]
            ++ hooks.enabledPackages;

          RUST_BACKTRACE = "1";

          inherit (hooks) shellHook;
        };
      }
    );
}
