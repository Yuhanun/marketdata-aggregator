{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { self
    , nixpkgs
    , rust-overlay
    ,
    }:
    let
      buildSystemConfig = system: additionalNativeBuildInputs: (
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          rustToolchain = pkgs.rust-bin.fromRustupToolchain {
            channel = "1.90.0";
            components = [ "rust-src" "rust-analyzer" "rustfmt" "rustc" "clippy" "cargo" "rust-docs" ];
          };
        in
        {
          default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              rustPlatform.bindgenHook
              pkgs.rust-analyzer-unwrapped
              protobuf_27
              protolint
            ] ++ additionalNativeBuildInputs pkgs;

            packages = [
              rustToolchain
            ];

            RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";

            RUST_BACKTRACE = "full";
            RUST_LOG = "INFO";
            CARGO_NET_GIT_FETCH_WITH_CLI = "true";
          };
        }
      );
    in
    {
      devShells.x86_64-linux = buildSystemConfig "x86_64-linux" (pkgs: [ ]);
      devShells.aarch64-darwin = buildSystemConfig "aarch64-darwin" (pkgs: [ ]);
    };
}
