# Implementing a New Data Loader

This guide provides step-by-step instructions for implementing a new data loader. Follow this checklist to ensure your loader integrates properly with the amp loader system.

## Overview

The loader system uses:
- **Generic base class**: `DataLoader[TConfig]` for type-safe configuration
- **Auto-discovery**: Loaders are automatically registered by the registry
- **Generalized tests**: Inherit from `BaseLoaderTests` and `BaseStreamingTests` for free test coverage

## Files to Create

| File | Purpose |
|------|---------|
| `src/amp/loaders/implementations/xxx_loader.py` | Main loader implementation |
| `tests/integration/loaders/backends/test_xxx.py` | Integration tests |

## Files to Modify

| File | Change |
|------|--------|
| `src/amp/loaders/implementations/__init__.py` | Add import with try/except |
| `tests/conftest.py` | Add testcontainer fixture and config fixture |

---

## Step 1: Create the Configuration Dataclass

```python
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class XxxConfig:
    """Configuration for Xxx loader."""

    # Required connection settings
    host: str = 'localhost'
    port: int = 1234
    database: str = 'default'
    username: str = 'default'
    password: str = ''

    # Performance settings
    batch_size: int = 10000

    # Optional: flexible connection params
    connection_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Merge connection_params into a usable format if needed
        pass
```

## Step 2: Create the Loader Class

```python
from typing import Any, Dict, Optional, Set

import pyarrow as pa

from ..base import DataLoader, LoadMode, LoadResult


class XxxLoader(DataLoader[XxxConfig]):
    """
    Data loader for Xxx database.

    Features:
    - Zero-copy data loading via PyArrow
    - Automatic table creation with type mapping
    - Streaming support with reorg handling
    """

    # Declare capabilities
    SUPPORTED_MODES: Set[LoadMode] = {LoadMode.APPEND}
    REQUIRES_SCHEMA_MATCH: bool = False
    SUPPORTS_TRANSACTIONS: bool = False  # Set True if DB supports atomic transactions

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        super().__init__(config, label_manager)
        self._client = None

    # ========== Required Methods ==========

    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            import xxx_driver
        except ImportError:
            raise ImportError(
                "xxx_driver package required. Install with: pip install xxx-driver"
            )

        self._client = xxx_driver.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            username=self.config.username,
            password=self.config.password,
            **self.config.connection_params,
        )
        self._is_connected = True
        self._record_connection_opened()

    def disconnect(self) -> None:
        """Close the database connection."""
        if self._client:
            self._client.close()
            self._client = None
        self._is_connected = False
        self._record_connection_closed()

    def _load_batch_impl(
        self,
        batch: pa.RecordBatch,
        table_name: str,
        **kwargs,
    ) -> int:
        """
        Load a single batch into the database.

        This is the core loading logic. The base class handles:
        - Table creation (calls _create_table_from_schema)
        - Retry logic and backpressure
        - Metrics recording

        Returns:
            Number of rows loaded
        """
        # Convert to table if needed (some drivers prefer Table over RecordBatch)
        table = pa.Table.from_batches([batch])

        # Use native driver insertion
        # Example: self._client.insert_arrow(table_name, table)

        return batch.num_rows

    # ========== Table Management ==========

    def _create_table_from_schema(
        self,
        schema: pa.Schema,
        table_name: str,
    ) -> None:
        """Create table with proper type mapping from Arrow schema."""
        columns = []

        for field in schema:
            # Skip internal metadata columns
            if field.name.startswith('_meta_'):
                continue

            db_type = self._arrow_to_xxx_type(field.type, field.nullable)
            columns.append(f"{field.name} {db_type}")

        # Create table with appropriate engine/options
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
        """
        self._client.execute(create_sql)

        # Create index on _amp_batch_id for fast reorg deletions
        if '_amp_batch_id' in [f.name for f in schema]:
            self._create_batch_id_index(table_name)

    def _arrow_to_xxx_type(self, arrow_type: pa.DataType, nullable: bool = True) -> str:
        """Map Arrow types to database types."""
        type_mapping = {
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INT',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'TINYINT UNSIGNED',
            pa.uint16(): 'SMALLINT UNSIGNED',
            pa.uint32(): 'INT UNSIGNED',
            pa.uint64(): 'BIGINT UNSIGNED',
            pa.float32(): 'FLOAT',
            pa.float64(): 'DOUBLE',
            pa.bool_(): 'BOOLEAN',
            pa.string(): 'VARCHAR',
            pa.large_string(): 'TEXT',
            pa.binary(): 'BLOB',
        }

        # Handle parameterized types
        if pa.types.is_timestamp(arrow_type):
            return 'TIMESTAMP'
        if pa.types.is_decimal(arrow_type):
            return f'DECIMAL({arrow_type.precision}, {arrow_type.scale})'
        if pa.types.is_list(arrow_type):
            inner = self._arrow_to_xxx_type(arrow_type.value_type, nullable=True)
            return f'ARRAY<{inner}>'

        base_type = type_mapping.get(arrow_type, 'VARCHAR')

        if nullable:
            return f'NULLABLE({base_type})'  # Adjust syntax for your DB
        return base_type

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""
        # Use system tables or information_schema
        result = self._client.query(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
        )
        return len(result) > 0

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Retrieve table schema as Arrow schema."""
        if not self.table_exists(table_name):
            return None

        # Query column info and convert to Arrow schema
        columns = self._client.query(f"DESCRIBE {table_name}")

        fields = []
        for col_name, col_type, *_ in columns:
            arrow_type = self._xxx_to_arrow_type(col_type)
            fields.append(pa.field(col_name, arrow_type))

        return pa.schema(fields)

    # ========== Streaming Support ==========

    def _handle_reorg(
        self,
        invalidation_ranges: list,
        table_name: str,
        connection_name: str,
    ) -> int:
        """
        Delete rows affected by blockchain reorganization.

        Uses _amp_batch_id column to identify and delete affected rows.
        The base class calls this when a reorg event is received.

        Returns:
            Number of rows deleted
        """
        # Find affected batch IDs from state store
        affected_batch_ids = self.state_store.get_affected_batch_ids(
            connection_name, invalidation_ranges
        )

        if not affected_batch_ids:
            return 0

        total_deleted = 0

        # Delete in chunks to avoid query size limits
        chunk_size = 1000
        for i in range(0, len(affected_batch_ids), chunk_size):
            chunk = affected_batch_ids[i:i + chunk_size]

            # Build IN clause for batch IDs
            batch_id_list = "', '".join(chunk)

            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE _amp_batch_id IN ('{batch_id_list}')
            """
            result = self._client.execute(delete_sql)
            total_deleted += result.rows_affected

        # Clear state for deleted batches
        for batch_id in affected_batch_ids:
            self.state_store.clear_batch(connection_name, batch_id)

        return total_deleted

    # ========== Optional: Transactional Loading ==========

    def load_batch_transactional(
        self,
        batch: pa.RecordBatch,
        table_name: str,
        batch_identifier: 'BatchIdentifier',
        ranges_complete: bool = False,
        **kwargs,
    ) -> LoadResult:
        """
        Load batch with exactly-once semantics using transactions.

        Only implement if SUPPORTS_TRANSACTIONS = True.
        """
        # Check if already processed
        if self.state_store.is_batch_processed(batch_identifier):
            return LoadResult(
                rows_loaded=0,
                success=True,
                # ... other fields
            )

        # Load within transaction
        with self._client.transaction():
            rows = self._load_batch_impl(batch, table_name, **kwargs)

            if ranges_complete:
                self.state_store.mark_batch_processed(batch_identifier)

        return LoadResult(rows_loaded=rows, success=True, ...)

    # ========== Utility Methods ==========

    def get_row_count(self, table_name: str) -> int:
        """Get number of rows in table."""
        result = self._client.query(f"SELECT COUNT(*) FROM {table_name}")
        return result[0][0]

    def health_check(self) -> Dict[str, Any]:
        """Check connection health."""
        if not self._is_connected or not self._client:
            return {'healthy': False, 'error': 'Not connected'}

        try:
            version = self._client.query("SELECT version()")
            return {
                'healthy': True,
                'server_version': version[0][0],
                'database': self.config.database,
            }
        except Exception as e:
            return {'healthy': False, 'error': str(e)}
```

## Step 3: Register the Loader

Edit `src/amp/loaders/implementations/__init__.py`:

```python
try:
    from .xxx_loader import XxxLoader
except ImportError:
    XxxLoader = None

# ... at bottom of file ...

if XxxLoader:
    __all__.append('XxxLoader')
```

## Step 4: Create Test Fixtures

Edit `tests/conftest.py`:

```python
# Add testcontainer import
from testcontainers.xxx import XxxContainer

# Add container fixture
@pytest.fixture(scope='session')
def xxx_container():
    """Xxx container for integration tests"""
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip('Testcontainers not available')

    container = XxxContainer(image='xxx/xxx-server:latest')
    container.start()

    # Wait for ready (adjust based on container logs)
    wait_for_logs(container, 'ready to accept connections', timeout=30)

    yield container
    container.stop()

# Add config fixture
@pytest.fixture
def xxx_test_config(request):
    """Xxx configuration from testcontainer or environment"""
    if TESTCONTAINERS_AVAILABLE and USE_TESTCONTAINERS:
        xxx_container = request.getfixturevalue('xxx_container')
        return {
            'host': xxx_container.get_container_host_ip(),
            'port': xxx_container.get_exposed_port(1234),
            'database': xxx_container.dbname,
            'username': xxx_container.username,
            'password': xxx_container.password,
            'batch_size': 10000,
        }
    else:
        return request.getfixturevalue('xxx_config')
```

## Step 5: Create Integration Tests

Create `tests/integration/loaders/backends/test_xxx.py`:

```python
"""
Xxx-specific loader integration tests.
"""

from typing import Any, Dict, List, Optional

import pytest

try:
    from src.amp.loaders.implementations.xxx_loader import XxxLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class XxxTestConfig(LoaderTestConfig):
    """Xxx-specific test configuration"""

    loader_class = XxxLoader
    config_fixture_name = 'xxx_test_config'

    # Set capability flags
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True
    requires_existing_table = True  # False if loader auto-creates tables

    def get_row_count(self, loader: XxxLoader, table_name: str) -> int:
        """Get row count from table"""
        return loader.get_row_count(table_name)

    def query_rows(
        self,
        loader: XxxLoader,
        table_name: str,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query rows from table"""
        query = f'SELECT * FROM {table_name}'
        if where:
            query += f' WHERE {where}'
        if order_by:
            query += f' ORDER BY {order_by}'

        result = loader._client.query(query)
        column_names = result.column_names
        return [dict(zip(column_names, row)) for row in result.rows]

    def cleanup_table(self, loader: XxxLoader, table_name: str) -> None:
        """Drop table"""
        try:
            loader._client.execute(f'DROP TABLE IF EXISTS {table_name}')
        except Exception:
            pass

    def get_column_names(self, loader: XxxLoader, table_name: str) -> List[str]:
        """Get column names from table"""
        result = loader._client.query(f'DESCRIBE {table_name}')
        return [row[0] for row in result.rows]


# Inherit generalized tests (6 core tests + 5 streaming tests)
@pytest.mark.xxx
class TestXxxCore(BaseLoaderTests):
    """Xxx core loader tests (inherited from base)"""
    config = XxxTestConfig()


@pytest.mark.xxx
class TestXxxStreaming(BaseStreamingTests):
    """Xxx streaming tests (inherited from base)"""
    config = XxxTestConfig()


# Add backend-specific tests
@pytest.mark.xxx
class TestXxxSpecific:
    """Xxx-specific tests that cannot be generalized"""

    def test_health_check(self, xxx_test_config):
        """Test health check functionality"""
        loader = XxxLoader(xxx_test_config)

        # Before connection
        health = loader.health_check()
        assert health['healthy'] == False

        # After connection
        with loader:
            health = loader.health_check()
            assert health['healthy'] == True

    def test_type_mapping(self, xxx_test_config, test_table_name, cleanup_tables):
        """Test Arrow to Xxx type mapping"""
        import pyarrow as pa

        cleanup_tables.append(test_table_name)

        data = {
            'int_col': pa.array([1, 2, 3], type=pa.int64()),
            'float_col': pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            'string_col': pa.array(['a', 'b', 'c'], type=pa.string()),
            'bool_col': pa.array([True, False, True], type=pa.bool_()),
        }
        table = pa.Table.from_pydict(data)

        loader = XxxLoader(xxx_test_config)
        with loader:
            result = loader.load_table(table, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 3
```

## Step 6: Add Makefile Target (Optional)

Edit `Makefile`:

```makefile
test-xxx:
	uv run pytest tests/integration/loaders/backends/test_xxx.py -v -m xxx
```

---

## Checklist

### Loader Implementation
- [ ] `XxxConfig` dataclass with connection and performance settings
- [ ] `XxxLoader` class inheriting `DataLoader[XxxConfig]`
- [ ] Class attributes: `SUPPORTED_MODES`, `REQUIRES_SCHEMA_MATCH`, `SUPPORTS_TRANSACTIONS`
- [ ] `connect()` with proper import error handling
- [ ] `disconnect()` with cleanup
- [ ] `_load_batch_impl()` - core loading logic
- [ ] `_create_table_from_schema()` with Arrow → DB type mapping
- [ ] `table_exists()` - check table existence
- [ ] `get_table_schema()` - retrieve schema as Arrow
- [ ] `get_row_count()` - row count utility
- [ ] `health_check()` - connection health

### Streaming Support (if applicable)
- [ ] `_handle_reorg()` - delete by `_amp_batch_id`
- [ ] Index on `_amp_batch_id` column in `_create_table_from_schema()`
- [ ] `load_batch_transactional()` (if `SUPPORTS_TRANSACTIONS = True`)

### Registration
- [ ] Import added to `src/amp/loaders/implementations/__init__.py`
- [ ] Added to `__all__` list

### Tests
- [ ] Testcontainer fixture in `tests/conftest.py`
- [ ] Config fixture in `tests/conftest.py`
- [ ] `XxxTestConfig(LoaderTestConfig)` with 4 abstract methods
- [ ] `TestXxxCore(BaseLoaderTests)` - inherits 6 generalized tests
- [ ] `TestXxxStreaming(BaseStreamingTests)` - inherits 5 streaming tests
- [ ] `TestXxxSpecific` - backend-specific tests (health check, type mapping, etc.)
- [ ] Pytest marker (`@pytest.mark.xxx`)

### Validation
- [ ] `make lint` passes
- [ ] `make format` applied
- [ ] Integration tests pass with testcontainers

---

## Common Patterns Reference

### Type Mapping (Arrow → SQL)

| Arrow Type | Common SQL | Notes |
|------------|------------|-------|
| `int8()` | `TINYINT` | |
| `int16()` | `SMALLINT` | |
| `int32()` | `INT` | |
| `int64()` | `BIGINT` | |
| `uint8()` | `TINYINT UNSIGNED` | |
| `uint16()` | `SMALLINT UNSIGNED` | |
| `uint32()` | `INT UNSIGNED` | |
| `uint64()` | `BIGINT UNSIGNED` | |
| `float32()` | `FLOAT` / `REAL` | |
| `float64()` | `DOUBLE` | |
| `bool_()` | `BOOLEAN` | |
| `string()` | `VARCHAR` / `TEXT` | |
| `binary()` | `BLOB` / `BYTEA` | |
| `timestamp()` | `TIMESTAMP` | Handle timezone |
| `date32()` | `DATE` | |
| `decimal128(p,s)` | `DECIMAL(p,s)` | Preserve precision |
| `list(T)` | `ARRAY<T>` | Recursive mapping |

### Metadata Columns

The base class automatically adds these columns for streaming data:
- `_amp_batch_id`: 16-char hex identifier for reorg deletion (always added when `store_batch_id=True`)
- `_amp_block_ranges`: Full JSON metadata (only when `store_full_metadata=True`)

Create an index on `_amp_batch_id` for fast deletion:
```python
# In _create_table_from_schema():
if '_amp_batch_id' in [f.name for f in schema]:
    self._client.execute(f"CREATE INDEX idx_{table_name}_batch_id ON {table_name}(_amp_batch_id)")
```

### Capability Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `SUPPORTED_MODES` | `{APPEND}` | Which load modes are supported |
| `REQUIRES_SCHEMA_MATCH` | `True` | Whether schema must match existing table |
| `SUPPORTS_TRANSACTIONS` | `False` | Whether atomic transactions are supported |

---

## Examples

See these existing implementations for reference:
- **ClickHouse**: `src/amp/loaders/implementations/clickhouse_loader.py` - OLAP, no transactions
- **PostgreSQL**: `src/amp/loaders/implementations/postgresql_loader.py` - OLTP, with transactions
- **Redis**: `src/amp/loaders/implementations/redis_loader.py` - Key-value, multiple data structures
- **DeltaLake**: `src/amp/loaders/implementations/deltalake_loader.py` - File-based, ACID transactions
