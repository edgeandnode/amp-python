# Python Amp Client

[![Unit tests status](https://github.com/edgeandnode/amp-python/actions/workflows/unit-tests.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/unit-tests.yml)
[![Integration tests status](https://github.com/edgeandnode/amp-python/actions/workflows/integration-tests.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/integration-tests.yml)
[![Formatting status](https://github.com/edgeandnode/amp-python/actions/workflows/ruff.yml/badge.svg?event=push)](https://github.com/edgeandnode/amp-python/actions/workflows/ruff.yml)


## Overview 

Client for issuing queries to an Amp server and working with the returned data.

## Dependencies
1. Rust
   `brew install rust`

## Installation
 
1. Ensure you have [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed locally.
2. Install dependencies
    ```bash
    uv build 
   ```
3. Activate a virtual environment

   Python 3.13 is the highest version supported `brew install python@3.13`
    ```bash
    uv venv --python 3.13
   ```

## Useage 

### Marimo

Start up a marimo workspace editor
```bash
uv run marimo edit
```

The Marimo app will open a new browser tab where you can create a new notebook, view helpful resources, and 
browse existing notebooks in the workspace.

### Apps

You can execute python apps and scripts using `uv run <path>` which will give them access to the dependencies 
and the `amp` package. For example, you can run the `execute_query` app with the following command.
```bash
uv run apps/execute_query.py
```

# Self-hosted Amp server

In order to operate a local Amp server you will need to have the files 
that [`dump`](../dump) produces available locally, and run the [server](../server) 
You can then use it in your python scripts, apps or notebooks.

# Testing

The project is set up to use the [`pytest`](https://docs.pytest.org/en/stable/) testing framework. 
It follows [standard python test discovery rules](https://docs.pytest.org/en/stable/explanation/goodpractices.html#test-discovery). 

## Quick Test Commands

Run all tests
```bash
uv run pytest
```

Run only unit tests (fast, no external dependencies)
```bash
make test-unit
```

Run integration tests with automatic container setup
```bash
make test-integration
```

Run all tests with coverage
```bash
make test-all
```

## Integration Testing

Integration tests can run in two modes:

### 1. Automatic Container Mode (Default)
The integration tests will automatically spin up PostgreSQL and Redis containers using testcontainers. This is the default mode and requires Docker to be installed and running.

```bash
# Run integration tests with automatic containers
uv run pytest tests/integration/ -m integration
```

**Note**: The configuration automatically disables Ryuk (testcontainers cleanup container) to avoid Docker connectivity issues. If you need Ryuk enabled, set `TESTCONTAINERS_RYUK_DISABLED=false`.

### 2. Manual Setup Mode
If you prefer to use your own database instances, you can disable testcontainers:

```bash
# Disable testcontainers and use manual configuration
export USE_TESTCONTAINERS=false

# Configure your database connections
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=test_amp
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=yourpassword

export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_PASSWORD=yourpassword  # Optional

# Run tests
uv run pytest tests/integration/ -m integration
```

For manual setup, you can use the provided Makefile commands:
```bash
# Start test databases manually
make test-setup

# Run tests
make test-integration

# Clean up databases
make test-cleanup
```

## Loader-Specific Tests

Run tests for specific loaders:
```bash
make test-postgresql   # PostgreSQL tests
make test-redis       # Redis tests
make test-deltalake   # Delta Lake tests
make test-iceberg     # Iceberg tests
make test-lmdb        # LMDB tests
```

# Linting and formatting

Ruff is configured to be used for linting and formatting of this project. 

Run formatter
```bash
uv run ruff format
```

Run linter 
```bash
uv run ruff check .
```

Run linter and apply auto-fixes
```bash 
uv run ruff check . --fix
```
