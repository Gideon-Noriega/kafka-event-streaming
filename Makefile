.PHONY: help infra-up infra-down topics-create schemas-register run produce consume test lint typecheck clean

SHELL := /bin/bash
BOOTSTRAP := localhost:19092
SCHEMA_REGISTRY := http://localhost:18081
RPK := docker exec -it redpanda rpk

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

infra-up: ## Start all infrastructure (Redpanda, Connect, PostgreSQL, Elasticsearch)
	docker compose up -d
	@echo "Waiting for Redpanda to be healthy..."
	@docker compose exec redpanda rpk cluster health --watch --exit-when-healthy

infra-down: ## Stop all infrastructure
	docker compose down

infra-reset: ## Stop and remove all data volumes
	docker compose down -v

topics-create: ## Create all Kafka topics with proper configuration
	$(RPK) topic create orders.placed \
		--partitions 12 \
		--config retention.ms=604800000 \
		--config min.insync.replicas=1
	$(RPK) topic create orders.enriched \
		--partitions 12 \
		--config retention.ms=604800000
	$(RPK) topic create orders.fraud_alerts \
		--partitions 3 \
		--config retention.ms=2592000000
	$(RPK) topic create orders.state_changes \
		--partitions 12 \
		--config retention.ms=2592000000
	$(RPK) topic create orders.dlq \
		--partitions 1 \
		--config retention.ms=7776000000
	$(RPK) topic create payments.processed \
		--partitions 6 \
		--config retention.ms=604800000
	$(RPK) topic create shipments.created \
		--partitions 6 \
		--config retention.ms=604800000
	$(RPK) topic create customers.profiles \
		--partitions 6 \
		--config cleanup.policy=compact \
		--config min.cleanable.dirty.ratio=0.1
	@echo "All topics created successfully."

topics-list: ## List all topics
	$(RPK) topic list

topics-describe: ## Describe all topics with configs
	$(RPK) topic describe orders.placed
	$(RPK) topic describe orders.enriched
	$(RPK) topic describe payments.processed

schemas-register: ## Register Avro schemas with Schema Registry
	@echo "Registering order_placed schema..."
	curl -s -X POST $(SCHEMA_REGISTRY)/subjects/orders.placed-value/versions \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "$$(python -c 'import json; f=open("src/schemas/order_placed_v1.avsc"); print(json.dumps({"schema": f.read()}))')"
	@echo "\nRegistering payment_processed schema..."
	curl -s -X POST $(SCHEMA_REGISTRY)/subjects/payments.processed-value/versions \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "$$(python -c 'import json; f=open("src/schemas/payment_processed.avsc"); print(json.dumps({"schema": f.read()}))')"
	@echo "\nRegistering shipment_created schema..."
	curl -s -X POST $(SCHEMA_REGISTRY)/subjects/shipments.created-value/versions \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "$$(python -c 'import json; f=open("src/schemas/shipment_created.avsc"); print(json.dumps({"schema": f.read()}))')"
	@echo "\nAll schemas registered."

schemas-list: ## List registered schemas
	curl -s $(SCHEMA_REGISTRY)/subjects | python -m json.tool

run: ## Run the full streaming pipeline
	python -m src.streams.app &
	python -m src.consumers.exactly_once &
	python -m src.consumers.group &
	@echo "Pipeline started. Use 'make produce' to send events."

produce: ## Produce sample order events
	python -m src.producers.idempotent

consume: ## Consume and display events from all topics
	$(RPK) topic consume orders.placed orders.enriched payments.processed --offset start

test: ## Run all tests
	pytest tests/ -v --cov=src --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/ -v -m unit

test-integration: ## Run integration tests (requires running infra)
	pytest tests/ -v -m integration

lint: ## Run linter
	ruff check src/ tests/
	ruff format --check src/ tests/

lint-fix: ## Fix linting issues
	ruff check --fix src/ tests/
	ruff format src/ tests/

typecheck: ## Run type checker
	mypy src/

clean: ## Remove build artifacts and caches
	rm -rf __pycache__ .pytest_cache .mypy_cache .coverage htmlcov dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
