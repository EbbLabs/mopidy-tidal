# Development guidelines
Please refer to this guide for development guidelines and instructions.

### Installation

- [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
- run `uv sync` to install all dependencies, including for development

- run `source path/to/venv/bin/activate` to activate the virtual environment in
  the current shell. Alternatively, prefix any command you want to run in the
  virtual environment with `uv run`, e.g. `uv run pytest` will run the tests.

## Test Suite
Mopidy-Tidal has a test suite which currently has 100% coverage.  Ideally
contributions would come with tests to keep this coverage up, but we can help in
writing them if need be.

Install using uv, and then run:

```bash
pytest tests
```

If you are on *nix, you can simply run:

```bash
make test
```

Currently the code is not very heavily documented.  The easiest way to see how
something is supposed to work is probably to have a look at the tests.


### Code Style
Code should be formatted with `ruff`:

```bash
ruff format
```

Additionally, run `ruff check` and clean up or `#noqa` its suggestions.  You may
want to use `ruff check --fix`, potentially after staging so you can see the
difference.

if you are on *nix you can run:

```bash
make format
```

The CI workflow will fail on linting as well as test failures.

### Installing a development version system-wide

```bash
rm -rf dist
uv build
pip install dist/*.whl
```

This installs the built package, without any of the development dependencies.
If you are on *nix you can just run:

```bash
make install
```

### Running mopidy against development code

Mopidy can be run against a local (development) version of Mopidy-Tidal.  There
are two ways to do this: using the system mopidy installation to provide audio
support, or installing `PyGObject` inside the virtualenv and ensuring it can
discover the required (compiled) libraries, which is normally as simple as
installing mopidy on system-wide to pull in all the gstreamer dependencies.

If you go the latter route, you might want to look at the code which runs our
integration tests in CI for inspiration.

In either case, run `mopidy` inside the virtualenv to launch mopidy with your
development version of Mopidy-Tidal.
