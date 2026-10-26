# Run from the repo root. Requires: uv, Docker (with compose v2), Java 21 + Maven.
UV       ?= uv
COMPOSE  ?= docker compose
# Load-test knobs (see README "Performance baseline").
RATE     ?= 1000
DURATION ?= 60s
P99_MS   ?= 25

.PHONY: test-unit test-integration test-java lint up down perf

test-unit:
	cd order-service && $(UV) run pytest tests/unit -q

test-integration:  ## needs Docker; Testcontainers starts Kafka + Postgres
	cd order-service && $(UV) run pytest tests/integration -q

test-java:
	cd streams-positions && mvn -q -B test

lint:
	cd order-service && $(UV) run ruff check .

up:
	$(COMPOSE) up -d --build --wait api processor

down:
	$(COMPOSE) --profile perf down -v --remove-orphans

# k6 exits non-zero when any threshold is breached, which fails this target.
perf: up
	RATE=$(RATE) DURATION=$(DURATION) P99_MS=$(P99_MS) $(COMPOSE) --profile perf run --rm k6
