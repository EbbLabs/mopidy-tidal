# Development guidelines
Please refer to this guide for development guidelines and instructions.

### Installation

- [Install poetry](https://python-poetry.org/docs/#installation)
- run `poetry install` to install all dependencies, including for development
- run `poetry shell` to activate the virtual environment

## Test Suite
Mopidy-Tidal has a test suite which currently has 100% coverage.  Ideally
contributions would come with tests to keep this coverage up, but we can help in
writing them if need be.

Install using poetry, and then run:

```bash
pytest tests/ -k "not gt_3_10" --cov=mopidy_tidal --cov-report=html
--cov-report=term-missing --cov-branch
```

substituting the correct python version (e.g. `-k "not gt_3.8"`).  This is
unlikely to be necessary beyond 3.9 as the python language has become very
standard.  It's only really needed to exclude a few tests which check that
dict-like objects behave the way modern dicts do, with `|`.

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
poetry build
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
support, or installing `PyGObject` inside the virtualenv.  The former is
recommended by Mopidy; the latter is used in our integration tests for two reasons:

- at the time of writing, poetry system-site-packages support is broken
- when integration testing, the version of PyGObject should be pinned (reproducible builds)

To install a completely isolated mopidy inside the virtualenv with `PyGObject`,
`mopidy-local` and `mopidy-iris` run

```bash
poetry install --with complete
```

This will compile a shim for gobject.  On any system other than a Raspberry
pi this will not take more than a minute, and is a once off.

Alternatively you can use a virtualenv which can see system-site-packages (which
still needs mopidy installed locally, as plugins use pip to register
themselves).  Until [#6035](https://github.com/python-poetry/poetry/issues/6035)
is resolved this requires a hack:

```bash
python -m venv .venv --system-site-packages
source ".venv/bin/activate" #or activate.csh or activate.fish or activate.ps1 as required
poetry install
```

*nix users can just run

```bash
make system-venv
source ".venv/bin/activate" #or activate.csh or activate.fish
```
(Make runs in a subshell and cannot modify the parent shell, so there's no way
by design of entering the venv permanently from within the makefile.)


In either case, run `mopidy` inside the virtualenv to launch mopidy with your
development version of Mopidy-Tidal.
