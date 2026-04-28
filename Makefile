COMPOSE = podman compose -f tests/docker-compose.yml
PYTEST = .venv/bin/pytest
RUFF = .venv/bin/ruff

.PHONY: all init test test-standalone format check lint compose-up compose-down

all: format check test

init:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt -r tests/requirements.txt
	.venv/bin/pip install -e .

format:
	$(RUFF) format src tests

check:
	$(RUFF) check src tests

lint: format check

test: compose-up
	$(PYTEST) -vv

test-standalone:
	$(PYTEST) -vv -m standalone

compose-up:
	@if [ -z "$$($(COMPOSE) ps -q 2>/dev/null)" ]; then $(COMPOSE) up -d --wait; fi

compose-down:
	$(COMPOSE) down
