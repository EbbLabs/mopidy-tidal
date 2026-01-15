.PHONY: lint test install format all system-venv integration-test
UV ?= uv run

help:
	@printf "Chose one of install, format, lint or test.\n"

install:
	rm -rf dist
	uv build --wheel
	pip install dist/*.whl

format:
	${UV} ruff format

lint:
	${UV} ruff check
	${UV} ruff format --check

system-venv:
	python -m venv .venv --system-site-packages
	bash -c "source .venv/bin/activate && uv install"
	@printf "You now need to activate the venv by sourcing the right file, e.g. source .venv/bin/activate\n"

test:
	${UV} pytest tests/ \
-k "not gt_$$(python3 --version | sed 's/Python \([0-9]\).\([0-9]*\)\..*/\1_\2/')" 

integration-test:
	${UV} pytest integration_tests/
