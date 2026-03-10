SHELL := /bin/bash
.PHONY: test test-unit test-integration test-all clean setup lint format generate-models

# Use UV for all commands
PYTHON = uv run --env-file .test.env

# Quick unit tests (no database dependencies)
test-unit:
	@echo "🧪 Running unit tests..."
	$(PYTHON) pytest tests/unit/ -m "unit" -v

# Integration tests (requires databases)
test-integration:
	@echo "🔗 Running integration tests..."
	$(PYTHON) pytest tests/integration/ -m "integration" -v --log-cli-level=CRITICAL

# All tests with coverage
test-all:
	@echo "🎯 Running all tests with coverage..."
	$(PYTHON) pytest --cov=src/amp --cov-report=html --cov-report=term-missing

# Specific loader tests
test-postgresql:
	@echo "🐘 Running PostgreSQL tests..."
	$(PYTHON) pytest tests/ -m "postgresql" -v --log-cli-level=ERROR

test-redis:
	@echo "🔴 Running Redis tests..."
	$(PYTHON) pytest tests/ -m "redis" -v --log-cli-level=ERROR

test-deltalake:
	@echo "🔺 Running Delta Lake tests..."
	$(PYTHON) pytest tests/ -m "delta_lake" -v --log-cli-level=ERROR

test-iceberg:
	@echo "🧊 Running iceberg tests..."
	$(PYTHON) pytest tests/ -m "iceberg" -v --log-cli-level=ERROR

test-snowflake:
	@echo "❄️ Running Snowflake tests..."
	$(PYTHON) pytest tests/ -m "snowflake" -v --log-cli-level=ERROR

test-lmdb:
	@echo "⚡ Running LMDB tests..."
	$(PYTHON) pytest tests/ -m "lmdb" -v --log-cli-level=ERROR

# Parallel streaming integration tests
test-parallel-streaming:
	@echo "⚡ Running parallel streaming integration tests..."
	$(PYTHON) pytest tests/integration/test_parallel_streaming.py -v -s --log-cli-level=INFO

# Performance tests
test-performance:
	@echo "🏇 Running performance tests..."
	$(PYTHON) pytest tests/performance/ -m "performance" -v --log-cli-level=ERROR

# E2E tests (require anvil, ampd, Docker)
test-e2e:
	@echo "Running E2E tests..."
	$(PYTHON) pytest tests/e2e/ -m "e2e" -v --log-cli-level=INFO -x --timeout=300

# Code quality (using your ruff config)
lint:
	@echo "🔍 Linting code..."
	$(PYTHON) ruff check .

lint-fix:
	@echo "🔍 Linting code..."
	$(PYTHON) ruff check . --fix

format:
	@echo "✨ Formatting code..."
	$(PYTHON) ruff format .

# Generate Pydantic models from OpenAPI spec
generate-models:
	@echo "🏗️  Generating Pydantic models from OpenAPI spec..."
	$(PYTHON) python scripts/generate_models.py

# Setup development environment
setup:
	@echo "🚀 Setting up development environment..."
	uv sync --all-groups

# Install specific loader dependencies
install-postgresql:
	@echo "📦 Installing PostgreSQL dependencies..."
	uv sync --group postgresql --group test --group dev

install-redis:
	@echo "📦 Installing Redis dependencies..."
	uv sync --group redis --group test --group dev

install-snowflake:
	@echo "📦 Installing Snowflake dependencies..."
	uv sync --group snowflake --group test --group dev

# Database setup for testing
test-setup:
	@echo "🐳 Starting test databases..."
	docker run -d --name test-postgres \
		-e POSTGRES_DB=test_db \
		-e POSTGRES_USER=test_user \
		-e POSTGRES_PASSWORD=test_pass \
		-p 5432:5432 postgres:13
	docker run -d --name test-redis -p 6379:6379 redis:7-alpine
	@echo "⏳ Waiting for databases to start..."
	sleep 10

# Cleanup test databases
test-cleanup:
	@echo "🧹 Cleaning up test databases..."
	-docker stop test-postgres test-redis
	-docker rm test-postgres test-redis

# Full test cycle with setup/cleanup (good for CI)
test-ci: test-setup test-all test-cleanup

# Clean test artifacts
clean:
	@echo "🧹 Cleaning test artifacts..."
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Show available commands
help:
	@echo "Available commands:"
	@echo "  make setup                    - Setup development environment"
	@echo "  make generate-models          - Generate Pydantic models from OpenAPI spec"
	@echo "  make test-unit                - Run unit tests (fast)"
	@echo "  make test-integration         - Run integration tests"
	@echo "  make test-parallel-streaming  - Run parallel streaming integration tests"
	@echo "  make test-all                 - Run all tests with coverage"
	@echo "  make test-postgresql          - Run PostgreSQL tests"
	@echo "  make test-redis               - Run Redis tests"
	@echo "  make test-snowflake           - Run Snowflake tests"
	@echo "  make test-performance         - Run performance tests"
	@echo "  make test-e2e                 - Run E2E tests (require anvil, ampd, Docker)"
	@echo "  make lint                     - Lint code with ruff"
	@echo "  make format                   - Format code with ruff"
	@echo "  make test-setup               - Start test databases"
	@echo "  make test-cleanup             - Stop test databases"
	@echo "  make clean                    - Clean test artifacts"
