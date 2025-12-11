{
  description = "Binix - A Nix binary cache server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        nativeBuildInputs = with pkgs; [
          pkg-config
          cmake
          perl
          go
          nasm
        ];

        # Use a compatible Nix version (2.24 or later)
        nix = pkgs.nixVersions.nix_2_28 or pkgs.nix;

        buildInputs =
          with pkgs;
          [
            openssl
            sqlite
            postgresql
            boost
            nix
            libarchive
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            darwin.apple_sdk.frameworks.Security
            darwin.apple_sdk.frameworks.SystemConfiguration
          ];

        devPackages = with pkgs; [
          # Rust toolchain
          rustc
          cargo
          rustfmt
          clippy
          rust-analyzer

          # Development utilities
          cargo-edit
          cargo-outdated
          cargo-audit

          # Testing and ops
          postgresql
          sqlite-interactive
          jq
        ];

        # Read version from release-please manifest
        version = (builtins.fromJSON (builtins.readFile ./.release-please-manifest.json)).server;

      in
      {
        packages = {
          default = self.packages.${system}.binix-client;

          binix-client = pkgs.rustPlatform.buildRustPackage {
            pname = "binix";
            inherit version nativeBuildInputs buildInputs;
            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
            };
            buildAndTestSubdir = "client";
          };

          binixd = pkgs.rustPlatform.buildRustPackage {
            pname = "binixd";
            inherit version nativeBuildInputs buildInputs;
            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
            };
            buildAndTestSubdir = "server";
          };

          docker-image = pkgs.dockerTools.buildImage {
            name = "binixd";
            tag = version;
            copyToRoot = [ self.packages.${system}.binixd ];

            config = {
              Entrypoint = [ "${self.packages.${system}.binixd}/bin/binixd" ];
              ExposedPorts = {
                "8080/tcp" = { };
              };
            };
          };
        };

        devShells.default = pkgs.mkShell {
          packages = devPackages ++ buildInputs ++ nativeBuildInputs;

          shellHook = ''
            echo "Binix development environment"
            echo "Rust version: $(rustc --version)"
            echo ""
            echo "Available commands:"
            echo "  cargo build          - Build the project"
            echo "  cargo test           - Run tests"
            echo "  cargo run -p binixd  - Run the server"
            echo "  cargo run -p binix   - Run the client"
            echo ""
          '';

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
          NIX_PATH = "nixpkgs=${pkgs.path}";
        };
      }
    );
}
