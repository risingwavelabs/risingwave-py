SHELL=/bin/bash

PROJECT_DIR=.
VENV_DIR=${PROJECT_DIR}/.venv

cleanup-venv:
	rm -rf ${VENV_DIR}

prepare-venv:
	uv sync

build:
	@rm -rf dist/*
	uv build

publish-test:
	uv publish --index testpypi

publish:
	uv publish
