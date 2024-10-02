{
  inputs.nixpkgs.url = "github:nixos/nixpkgs/release-24.05";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.zls-flake.url = "github:zigtools/zls";
  outputs = { nixpkgs, flake-utils, zls-flake, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              zig-pkgs = zls-flake.inputs.zig-overlay.packages.${system};
              zls = zls-flake.packages.${system}.default;
            })
          ];
        };
        fhs = let my-python = pkgs.python311;
        in pkgs.buildFHSUserEnv {
          name = "fhs-shell";
          targetPkgs = p:
            with p; [
              zig-pkgs."0.13.0"
              zls
              gdb
            ];
        };
      in { devShell = fhs.env; });
}
