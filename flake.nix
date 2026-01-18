{
  description = "Mopidy Extension for Tidal music service integration.";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem
    (
      system: let
        pkgs = import nixpkgs {
          inherit system;
        };
        python = pkgs.python313;
        buildInputs =
          (with pkgs; [
            (python.withPackages (ps:
              with ps; [
                gst-python
                pygobject3
              ]))
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
        env = {
          UV_PROJECT_ENVIRONMENT = ".direnv/venv";
          # libsoup_3 is broken, and why wouldn't you use curl?
          GST_PLUGIN_FEATURE_RANK = "curlhttpsrc:MAX";
        };
      in
        with pkgs; {
          devShells.default = mkShell {
            inherit buildInputs;
            inherit env;
            shellHook = ''
              # pre-commit install
              [ ! -d $UV_PROJECT_ENVIRONMENT ] && uv venv $UV_PROJECT_ENVIRONMENT --python ${python}/bin/python
              source $UV_PROJECT_ENVIRONMENT/bin/activate
            '';
          };
        }
    );
}
