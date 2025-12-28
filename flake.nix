{
  description = "gstreamer proxy";

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
        # 'build time' deps
        buildInputs =
          (with pkgs; [
            (python.withPackages (ps:
              with ps; [
                gst-python
                pygobject3
                # packages not specified in pyproject.toml: these will be available in the venv.
              ]))
            uv
            pre-commit
            ruff
            gobject-introspection
          ])
          ++ (with pkgs.gst_all_1; [
            # glib-networking
            gst-plugins-bad
            gst-plugins-base
            gst-plugins-good
            gst-plugins-ugly
            gst-plugins-rs
          ]);
        env = {
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
            pkgs.stdenv.cc.cc # allow building c extensions
          ];
          UV_PROJECT_ENVIRONMENT = ".direnv/venv";
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
