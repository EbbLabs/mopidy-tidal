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
        buildInputs =
          (with pkgs; [
            (python.withPackages (ps:
              with ps; [
                gst-python
                pygobject3
              ]))
            uv
            pre-commit
            ruff
            mopidy # for its build inputs: it would be nice to do this properly, but I can't seem to get network playing to work
          ])
          ++ (with pkgs.gst_all_1; [
            pkgs.glib-networking
            gst-plugins-bad
            gst-plugins-base
            gst-plugins-good
            gst-plugins-ugly
            gst-plugins-rs
          ]);
        env = {
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
