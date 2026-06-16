.PHONY: install install-hooks lint format-check format typecheck check test coverage smoke test-docker test-docker-tracing server server-docker up down logs stack-reset jaeger search-attributes worker llm-worker api doctor ticket status approve reject batch reset

N ?= 100
API_URL ?= http://localhost:8000
JAEGER_URL ?= http://localhost:16686
TEMPORAL_NAMESPACE ?= default

install:
	uv sync
	uv run pre-commit install --hook-type pre-push

install-hooks:
	uv run pre-commit install --hook-type pre-push

lint:
	uv run ruff check .

format-check:
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run pyright

check: format-check lint typecheck test

test:
	uv run pytest

coverage:
	uv run pytest --cov=ticketflow --cov-report=term-missing

## --- deployment smoke tests (against a running docker stack) ---

smoke:
	API_URL=$(API_URL) uv run pytest tests/test_smoke_stack.py -o addopts=

test-docker: up
	API_URL=$(API_URL) uv run pytest tests/test_smoke_stack.py -o addopts=
	docker compose down

test-docker-tracing:
	TICKETFLOW_TRACE_EXPORTER=otlp COMPOSE_PROFILES=tracing docker compose up --build -d
	API_URL=$(API_URL) uv run pytest tests/test_smoke_stack.py -o addopts=
	API_URL=$(API_URL) JAEGER_URL=$(JAEGER_URL) uv run pytest tests/test_tracing_stack.py -o addopts=
	COMPOSE_PROFILES=tracing docker compose down

## --- run the stack (one target per terminal) ---

server:
	temporal server start-dev

server-docker:
	docker compose up temporal temporal-init jaeger

## --- full stack in docker (server, workers, api in one command) ---

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

stack-reset:
	docker compose down -v

jaeger:
	docker compose up jaeger

search-attributes:
	temporal operator search-attribute create --namespace $(TEMPORAL_NAMESPACE) --name TicketStatus --type Keyword

worker:
	uv run python -m ticketflow.worker

llm-worker:
	MOCK_AGENT_LATENCY_MAX_S=3 uv run python -m ticketflow.llm_worker

api:
	uv run uvicorn ticketflow.api:app --reload

doctor:
	uv run python scripts/doctor.py

## --- drive a ticket through (usage: make ticket / make status ID=abc123) ---

ticket:
	@uv run python scripts/doctor.py --quiet --base-url $(API_URL)
	curl -s -X POST $(API_URL)/tickets \
	  -H 'Content-Type: application/json' \
	  -d '{"customer_email": "jo@example.com", "subject": "refund please", "body": "I was double charged."}'

status:
	curl -s $(API_URL)/tickets/$(ID)

approve:
	curl -s -X POST $(API_URL)/tickets/$(ID)/approval \
	  -H 'Content-Type: application/json' \
	  -d '{"approved": true, "approver": "make", "note": "approved via make"}'

reject:
	curl -s -X POST $(API_URL)/tickets/$(ID)/approval \
	  -H 'Content-Type: application/json' \
	  -d '{"approved": false, "approver": "make", "note": "rejected via make"}'

batch:
	uv run python scripts/batch.py --count $(N) --base-url $(API_URL)

reset:
	uv run python scripts/reset.py
