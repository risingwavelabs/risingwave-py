SHELL=/bin/bash

PROJECT_DIR=.
VENV_DIR=${PROJECT_DIR}/venv
PYTHON3_BIN=${VENV_DIR}/bin/python3

venv-freeze:
	$(PYTHON3_BIN) -m pip freeze > ${PROJECT_DIR}/requirements-dev.txt

cleanup-venv:
	rm -rf ${VENV_DIR}

prepare-venv:
	python3 -m venv ${VENV_DIR}
	source ${VENV_DIR}/bin/activate
	$(PYTHON3_BIN) -m pip install -r ${PROJECT_DIR}/requirements-dev.txt
	@echo
	@echo "=============================================="
	@echo 'use the following statements to activate venv:'
	@echo
	@echo 'source ${VENV_DIR}/bin/activate'
	@echo "=============================================="

build:
	@rm -rf dist/*
	source ${VENV_DIR}/bin/activate
	hatch build

publish-test:
	source ${VENV_DIR}/bin/activate
	$(PYTHON3_BIN) -m twine upload --repository testpypi dist/*

publish:
	source ${VENV_DIR}/bin/activate
	$(PYTHON3_BIN) -m twine upload dist/*
