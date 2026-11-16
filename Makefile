# dx.backend: the same commands as every DX repo (`make help`).
# `make setup` once, then `make dev` runs the backend on this machine. To run the
# whole DX stack, in Docker or not, use infra.dx.

PYTHON ?= 3.11
VENV := .venv
IMAGE ?= dx-backend:local

.DEFAULT_GOAL := help
.PHONY: help setup dev build logs lint

help: ## List the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

setup: ## Create a Python 3.11 virtualenv in .venv with the dependencies, create .env
	@if command -v uv >/dev/null; then \
	  [ -d $(VENV) ] || uv venv --python $(PYTHON) $(VENV); \
	  uv pip install --python $(VENV)/bin/python -r requirements.txt; \
	else \
	  [ -d $(VENV) ] || python$(PYTHON) -m venv $(VENV); \
	  $(VENV)/bin/pip install -r requirements.txt; \
	fi
	@[ -f .env ] || { cp .env.example .env && echo "Created .env from .env.example"; }

dev: ## Run the backend on http://localhost:4004, reloading on changes
	@[ -x $(VENV)/bin/flask ] || { echo "No virtualenv yet: run make setup." >&2; exit 1; }
	$(VENV)/bin/flask --app app run --port 4004 --debug

build: ## Build the Docker image, as CI does
	docker build -t $(IMAGE) .

logs: ## Follow the backend's log file
	tail -f logging/backend.log

lint: ## Check code style
	$(VENV)/bin/flake8 .
