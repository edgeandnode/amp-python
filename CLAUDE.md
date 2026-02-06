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
- Generic `DataLoader[TConfig]` base class in `src/amp/loaders/base.py`
- Auto-discovery via registry in `src/amp/loaders/registry.py`
- Zero-copy operations using PyArrow for performance
- Built-in resilience (retry, backpressure), state management, and reorg handling

**For detailed implementation instructions, see `src/amp/loaders/NEW_LOADER_GUIDE.md`**

#### Quick Reference: Implementing New Loaders

**Files to create:**
1. `src/amp/loaders/implementations/xxx_loader.py` - Main implementation
2. `tests/integration/loaders/backends/test_xxx.py` - Integration tests

**Files to modify:**
1. `src/amp/loaders/implementations/__init__.py` - Add import
2. `tests/conftest.py` - Add testcontainer and config fixtures

**Required implementation:**
```python
@dataclass
class XxxConfig:
    host: str = 'localhost'
    port: int = 1234
    # ... connection settings

class XxxLoader(DataLoader[XxxConfig]):
    SUPPORTED_MODES = {LoadMode.APPEND}
    SUPPORTS_TRANSACTIONS = False

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def _load_batch_impl(self, batch, table_name, **kwargs) -> int: ...
    def _create_table_from_schema(self, schema, table_name) -> None: ...
    def table_exists(self, table_name) -> bool: ...
```

**Test implementation:**
```python
class XxxTestConfig(LoaderTestConfig):
    loader_class = XxxLoader
    config_fixture_name = 'xxx_test_config'

    def get_row_count(self, loader, table_name) -> int: ...
    def query_rows(self, loader, table_name, where, order_by) -> List[Dict]: ...
    def cleanup_table(self, loader, table_name) -> None: ...
    def get_column_names(self, loader, table_name) -> List[str]: ...

class TestXxxCore(BaseLoaderTests):
    config = XxxTestConfig()  # Inherits 6 generalized tests

class TestXxxStreaming(BaseStreamingTests):
    config = XxxTestConfig()  # Inherits 5 streaming tests
```

#### Existing Loaders (for reference)
- **ClickHouse**: OLAP, columnar, no transactions - `clickhouse_loader.py`
- **PostgreSQL**: OLTP, connection pooling, transactions - `postgresql_loader.py`
- **Redis**: Key-value, multiple data structures - `redis_loader.py`
- **Snowflake**: Cloud warehouse - `snowflake_loader.py`
- **DeltaLake**: File-based, ACID transactions - `deltalake_loader.py`
- **Iceberg**: Catalog-based, partitioned tables - `iceberg_loader.py`
- **LMDB**: Embedded, memory-mapped - `lmdb_loader.py`

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