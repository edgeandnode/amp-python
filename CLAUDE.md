# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Python client library for Project amp - a system for querying a amp server (Flight SQL based) and loading the returned data into various storage systems. The library focuses on zero-copy operations and high-performance data streaming.

## Key Commands

### Development Setup
```bash
# Install dependencies and build
uv build

# Activate virtual environment
uv venv

# Run Marimo notebook environment
uv run marimo edit
```

### Testing
```bash
# Quick unit tests (no database dependencies)
make test-unit

# All tests with coverage
make test-all

# Specific loader tests
make test-postgresql
make test-redis

# Run a single test file
uv run pytest tests/unit/test_postgresql_loader.py -v

# Run a single test function
uv run pytest tests/unit/test_postgresql_loader.py::TestPostgreSQLLoader::test_connection -v
```

### Code Quality
```bash
# Format code
make format
# or
uv run ruff format

# Lint code
make lint
# or
uv run ruff check .

# Auto-fix linting issues
uv run ruff check . --fix
```

## Architecture

### Data Loader System
The core architecture follows a plugin-based loader system with:
- Abstract `DataLoader` base class in `src/amp/loaders/base.py`
- Auto-discovery mechanism for loaders via `__init_subclass__`
- Zero-copy operations using PyArrow for performance
- Connection management with named connections and environment variables

When implementing new loaders:
1. Inherit from `DataLoader` base class
2. Implement required methods: `connect()`, `load_table()`, `close()`
3. Define configuration schema using dataclasses
4. Register supported data types in class attributes
5. Follow existing patterns from PostgreSQL and Redis loaders

### Testing Strategy
- **Unit tests**: Test pure logic and data structures WITHOUT mocking. Unit tests should be simple, fast, and test isolated components (dataclasses, utility functions, partitioning logic, etc.). Do NOT add tests that require mocking to `tests/unit/`.
- **Integration tests**: Use testcontainers for real database testing. Tests that require external dependencies (databases, Flight SQL server, etc.) belong in `tests/integration/`.
- **Performance tests**: Benchmark data loading operations
- Tests can be filtered using pytest markers (e.g., `-m unit` for unit tests only)

### Key Dependencies
- **pyarrow**: Core data processing and zero-copy operations
- **Flight SQL**: Protocol for querying amp server
- **Database drivers**: psycopg2 (PostgreSQL), redis
- **Testing**: pytest with testcontainers for integration tests

## Important Notes
- Always use `uv run` prefix for Python commands to ensure proper environment
- Integration tests require Docker for database containers
- The project uses Ruff for both linting and formatting with specific per-file ignores
- Protocol buffer files (`FlightSql_pb2.py`) are excluded from linting