{
  description = "Mopidy Extension for Tidal music service integration.";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    flake-parts,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = nixpkgs.lib.systems.flakeExposed;
      perSystem = {
        pkgs,
        system,
        ...
      }: let
        python = pkgs.python3.withPackages (ps: [ps.gst-python ps.pygobject3]);
        buildInputs =
          [
            python
          ]
          ++ (with pkgs; [
            # dev
            uv
            pre-commit
            ruff
            # deps
            mopidy # for its build inputs, and local testing
            gobject-introspection
            glib-networking
            # integration tests
            mpc
            mopidy-mpd
            # local testing
            mopidy-local
            mopidy-iris
          ])
          ++ (with pkgs.gst_all_1; [
            gst-plugins-bad
            gst-plugins-base
            gst-plugins-good
            gst-plugins-ugly
            gst-plugins-rs
          ]);
      in {
        devShells.default = pkgs.mkShell {
          inherit buildInputs;
          env = {
            UV_PROJECT_ENVIRONMENT = ".direnv/venv";
            # libsoup_3 is broken, and why wouldn't you use curl?
            GST_PLUGIN_FEATURE_RANK = "curlhttpsrc:MAX";
          };
          shellHook = ''
            # pre-commit install
            [ ! -d $UV_PROJECT_ENVIRONMENT ] && uv venv $UV_PROJECT_ENVIRONMENT --python ${python}/bin/python
            source $UV_PROJECT_ENVIRONMENT/bin/activate
          '';
        };
      };
    };
}
