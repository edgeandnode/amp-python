# Implementing Data Loaders

This guide provides a comprehensive walkthrough for implementing new Data Loaders in the Project amp Python client library.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Getting Started](#getting-started)
4. [Implementation Guide](#implementation-guide)
5. [Configuration](#configuration)
6. [Metadata Methods](#metadata-methods)
7. [Testing](#testing)
8. [Best Practices](#best-practices)
9. [Examples](#examples)

## Overview

Data Loaders are plugins that enable loading Arrow data into various storage systems. The architecture is designed for:

- **Zero-copy operations** using PyArrow for performance
- **Auto-discovery** mechanism via `__init_subclass__`
- **Standardized interfaces** across all loaders
- **Type-safe configuration** with dataclasses
- **Comprehensive error handling** and metadata collection

## Architecture

### Base Class Hierarchy

```
DataLoader[TConfig] (ABC, Generic)
├── PostgreSQLLoader[PostgreSQLConfig]
├── RedisLoader[RedisConfig]
├── SnowflakeLoader[SnowflakeConnectionConfig]
├── DeltaLakeLoader[DeltaLakeStorageConfig]
├── IcebergLoader[IcebergStorageConfig]
└── LMDBLoader[LMDBConfig]
```

### Key Components

1. **DataLoader**: Generic base class with common functionality
2. **LoadMode**: Enum for load operations (APPEND, OVERWRITE, UPSERT, MERGE)
3. **LoadResult**: Standardized result object with metadata
4. **Auto-discovery**: Automatic registration via class inheritance

## Getting Started

### 1. Create Loader Class

Create a new file in `src/amp/loaders/implementations/` following the naming pattern `{system}_loader.py`:

```python
# src/amp/loaders/implementations/example_loader.py

from dataclasses import dataclass
from typing import Any, Dict
import pyarrow as pa
from ..base import DataLoader, LoadMode

@dataclass
class ExampleConfig:
    host: str
    port: int = 5432
    database: str
    timeout: int = 30

class ExampleLoader(DataLoader[ExampleConfig]):
    """Example loader implementation"""
    
    # Declare supported capabilities
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True
    
    def _parse_config(self, config: Dict[str, Any]) -> ExampleConfig:
        return ExampleConfig(**config)
    
    def _get_required_config_fields(self) -> list[str]:
        return ['host', 'database']
    
    def connect(self) -> None:
        # Implementation here
        self._is_connected = True
    
    def disconnect(self) -> None:
        # Implementation here
        self._is_connected = False
    
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        # Implementation here - return number of rows loaded
        return batch.num_rows
```

### 2. Register for Auto-Discovery

Add import to `src/amp/loaders/implementations/__init__.py`:

```python
try:
    from .example_loader import ExampleLoader
except ImportError:
    ExampleLoader = None

if ExampleLoader:
    __all__.append('ExampleLoader')
```

The loader will automatically be registered and available as `'example'`.

## Implementation Guide

### Required Methods

#### 1. `_parse_config(self, config: Dict[str, Any]) -> TConfig`

Parse configuration into typed format:

```python
def _parse_config(self, config: Dict[str, Any]) -> ExampleConfig:
    try:
        return ExampleConfig(**config)
    except (TypeError, KeyError) as e:
        raise ValueError(f"Invalid configuration: {e}")
```

#### 2. `connect(self) -> None`

Establish connection to your target system:

```python
def connect(self) -> None:
    try:
        self._connection = create_connection(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database
        )
        
        # Test connection and log info
        info = self._connection.get_info()
        self.logger.info(f"Connected to {info['system']} v{info['version']}")
        
        self._is_connected = True
        
    except Exception as e:
        self.logger.error(f"Failed to connect: {e}")
        raise
```

#### 3. `disconnect(self) -> None`

Clean up connections:

```python
def disconnect(self) -> None:
    if self._connection:
        self._connection.close()
        self._connection = None
    self._is_connected = False
    self.logger.info("Disconnected")
```

#### 4. `_load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int`

Core loading logic - the base class handles everything else:

```python
def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
    # Base class already handled:
    # - Connection checking
    # - Mode validation  
    # - Table creation (_create_table_from_schema)
    # - Overwrite clearing (_clear_table)
    # - Error handling and LoadResult creation
    # - Timing and metadata collection
    
    # Just implement the actual data loading
    data_dict = batch.to_pydict()
    rows_written = 0
    
    for i in range(batch.num_rows):
        # Process each row using zero-copy Arrow operations
        row_data = {col: data_dict[col][i] for col in data_dict.keys()}
        self._connection.insert(table_name, row_data)
        rows_written += 1
    
    return rows_written
```

### Optional Methods

#### Table Management

```python
def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
    """Create table from Arrow schema"""
    columns = []
    for field in schema:
        if pa.types.is_timestamp(field.type):
            sql_type = 'TIMESTAMP'
        elif pa.types.is_int64(field.type):
            sql_type = 'BIGINT'
        elif pa.types.is_string(field.type):
            sql_type = 'VARCHAR'
        else:
            sql_type = 'VARCHAR'  # Safe fallback
        
        nullable = '' if field.nullable else ' NOT NULL'
        columns.append(f'"{field.name}" {sql_type}{nullable}')
    
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    self._connection.execute(sql)

def _clear_table(self, table_name: str) -> None:
    """Clear table for overwrite mode"""
    self._connection.execute(f"DELETE FROM {table_name}")
```

#### Introspection

```python
def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
    """Get table information"""
    try:
        return {
            'table_name': table_name,
            'row_count': self._get_row_count(table_name),
            'columns': self._get_column_info(table_name),
            'size_bytes': self._get_table_size(table_name)
        }
    except Exception as e:
        self.logger.error(f"Failed to get table info: {e}")
        return None
```

## Configuration

### Using Dataclasses (Recommended)

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExampleConfig:
    host: str
    port: int = 5432
    database: str
    user: str
    password: str
    timeout: Optional[int] = None
    max_connections: int = 10

class ExampleLoader(DataLoader[ExampleConfig]):
    def _parse_config(self, config: Dict[str, Any]) -> ExampleConfig:
        try:
            return ExampleConfig(**config)
        except (TypeError, KeyError) as e:
            raise ValueError(f"Invalid configuration: {e}")
    
    def _get_required_config_fields(self) -> list[str]:
        return ['host', 'database', 'user', 'password']
```

## Metadata Methods

Both metadata methods are **required** and must include specific fields for consistency across loaders.

### `_get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]`

```python
def _get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
    """Get metadata for batch operation"""
    return {
        'operation': 'load_batch',  # REQUIRED
        'batch_size': batch.num_rows,
        'schema_fields': len(batch.schema),
        'throughput_rows_per_sec': round(batch.num_rows / duration, 2) if duration > 0 else 0,
        # Add loader-specific fields
        'loading_method': self.config.method,
        'connection_pool_size': self.config.max_connections
    }
```

### `_get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]`

```python
def _get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
    """Get metadata for table operation"""
    return {
        'operation': 'load_table',  # REQUIRED
        'batch_count': batch_count,
        'batches_processed': batch_count,  # REQUIRED for some tests
        'total_rows': table.num_rows,
        'schema_fields': len(table.schema),
        'avg_batch_size': round(table.num_rows / batch_count, 2) if batch_count > 0 else 0,
        'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
        'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0,
        # Add loader-specific fields
        'loading_method': self.config.method
    }
```

## Testing

### Generalized Test Infrastructure

The project uses a generalized test infrastructure that eliminates code duplication across loader tests. Instead of writing standalone tests for each loader, you inherit from shared base test classes.

### Architecture

```
tests/integration/loaders/
├── conftest.py               # Base classes and fixtures
├── test_base_loader.py       # 7 core tests (all loaders inherit)
├── test_base_streaming.py    # 5 streaming tests (for loaders with reorg support)
└── backends/
    ├── test_postgresql.py    # PostgreSQL-specific config + tests
    ├── test_redis.py         # Redis-specific config + tests
    └── test_example.py       # Your loader tests here
```

### Step 1: Create Configuration Fixture

Add your loader's configuration fixture to `tests/conftest.py`:

```python
@pytest.fixture(scope='session')
def example_test_config(request):
    """Example loader configuration from testcontainer or environment"""
    # Use testcontainers for CI, or fall back to environment variables
    if TESTCONTAINERS_AVAILABLE and USE_TESTCONTAINERS:
        # Set up testcontainer (if applicable)
        example_container = request.getfixturevalue('example_container')
        return {
            'host': example_container.get_container_host_ip(),
            'port': example_container.get_exposed_port(5432),
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
        }
    else:
        # Fall back to environment variables
        return {
            'host': os.getenv('EXAMPLE_HOST', 'localhost'),
            'port': int(os.getenv('EXAMPLE_PORT', '5432')),
            'database': os.getenv('EXAMPLE_DB', 'test_db'),
            'user': os.getenv('EXAMPLE_USER', 'test_user'),
            'password': os.getenv('EXAMPLE_PASSWORD', 'test_pass'),
        }
```

### Step 2: Create Test Configuration Class

Create `tests/integration/loaders/backends/test_example.py`:

```python
"""
Example loader integration tests using generalized test infrastructure.
"""

from typing import Any, Dict, List, Optional
import pytest

from src.amp.loaders.implementations.example_loader import ExampleLoader
from tests.integration.loaders.conftest import LoaderTestConfig
from tests.integration.loaders.test_base_loader import BaseLoaderTests
from tests.integration.loaders.test_base_streaming import BaseStreamingTests


class ExampleTestConfig(LoaderTestConfig):
    """Example-specific test configuration"""

    loader_class = ExampleLoader
    config_fixture_name = 'example_test_config'

    # Declare loader capabilities
    supports_overwrite = True
    supports_streaming = True      # Set to False if no streaming support
    supports_multi_network = True  # For blockchain loaders with reorg
    supports_null_values = True

    def get_row_count(self, loader: ExampleLoader, table_name: str) -> int:
        """Get row count from table"""
        # Implement using your loader's API
        return loader._connection.query(f"SELECT COUNT(*) FROM {table_name}")[0]['count']

    def query_rows(
        self,
        loader: ExampleLoader,
        table_name: str,
        where: Optional[str] = None,
        order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from table"""
        query = f"SELECT * FROM {table_name}"
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        return loader._connection.query(query)

    def cleanup_table(self, loader: ExampleLoader, table_name: str) -> None:
        """Drop table"""
        loader._connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    def get_column_names(self, loader: ExampleLoader, table_name: str) -> List[str]:
        """Get column names from table"""
        result = loader._connection.query(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        )
        return [row['column_name'] for row in result]


# Core tests - ALL loaders must inherit these
class TestExampleCore(BaseLoaderTests):
    """Inherits 7 core tests: connection, context manager, batching, modes, null handling, errors"""
    config = ExampleTestConfig()


# Streaming tests - Only for loaders with streaming/reorg support
class TestExampleStreaming(BaseStreamingTests):
    """Inherits 5 streaming tests: metadata columns, reorg deletion, overlapping ranges, multi-network, microbatch dedup"""
    config = ExampleTestConfig()


# Loader-specific tests
@pytest.mark.integration
@pytest.mark.example
class TestExampleSpecific:
    """Example-specific functionality tests"""
    config = ExampleTestConfig()

    def test_custom_feature(self, loader, test_table_name, cleanup_tables):
        """Test example-specific functionality"""
        cleanup_tables.append(test_table_name)

        with loader:
            # Test your loader's unique features
            result = loader.some_custom_method(test_table_name)
            assert result.success
```

### What You Get Automatically

By inheriting from the base test classes, you automatically get:

**From `BaseLoaderTests` (7 core tests):**
- `test_connection` - Connection establishment and disconnection
- `test_context_manager` - Context manager functionality
- `test_batch_loading` - Basic batch loading
- `test_append_mode` - Append mode operations
- `test_overwrite_mode` - Overwrite mode operations
- `test_null_handling` - Null value handling
- `test_error_handling` - Error scenarios

**From `BaseStreamingTests` (5 streaming tests):**
- `test_streaming_metadata_columns` - Metadata column creation
- `test_reorg_deletion` - Blockchain reorganization handling
- `test_reorg_overlapping_ranges` - Overlapping range invalidation
- `test_reorg_multi_network` - Multi-network reorg isolation
- `test_microbatch_deduplication` - Microbatch duplicate detection

### Required LoaderTestConfig Methods

You must implement these four methods in your `LoaderTestConfig` subclass:

```python
def get_row_count(self, loader, table_name: str) -> int:
    """Return number of rows in table"""

def query_rows(self, loader, table_name: str, where=None, order_by=None) -> List[Dict]:
    """Query and return rows as list of dicts"""

def cleanup_table(self, loader, table_name: str) -> None:
    """Drop/delete the table"""

def get_column_names(self, loader, table_name: str) -> List[str]:
    """Return list of column names"""
```

### Capability Flags

Set these flags in your `LoaderTestConfig` to control which tests run:

```python
supports_overwrite = True       # Can overwrite existing data
supports_streaming = True       # Supports streaming with metadata
supports_multi_network = True   # Supports multi-network isolation (blockchain loaders)
supports_null_values = True     # Handles NULL values correctly
```

### Running Tests

```bash
# Run all tests for your loader
uv run pytest tests/integration/loaders/backends/test_example.py -v

# Run only core tests
uv run pytest tests/integration/loaders/backends/test_example.py::TestExampleCore -v

# Run only streaming tests
uv run pytest tests/integration/loaders/backends/test_example.py::TestExampleStreaming -v

# Run specific test
uv run pytest tests/integration/loaders/backends/test_example.py::TestExampleCore::test_connection -v
```

## Best Practices

### 1. Performance

- **Use Arrow directly**: Avoid unnecessary pandas conversions
- **Batch operations**: Minimize network round trips
- **Zero-copy when possible**: Use `batch.to_pydict()` for efficient conversion
- **Connection pooling**: Reuse connections for multiple operations

### 2. Error Handling

```python
def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
    rows_loaded = 0
    errors = []
    
    try:
        for i in range(batch.num_rows):
            try:
                # Process row
                rows_loaded += 1
            except Exception as e:
                errors.append(f"Row {i}: {e}")
                if len(errors) > 100:  # Reasonable limit
                    raise Exception(f"Too many errors: {len(errors)}")
        
        if errors:
            self.logger.warning(f"Completed with {len(errors)} errors")
        
        # Important: Report failure if no rows loaded but errors exist
        if rows_loaded == 0 and errors:
            error_summary = errors[:5]  # Show first 5 errors
            if len(errors) > 5:
                error_summary.append(f"... and {len(errors) - 5} more errors")
            raise Exception(f"Failed to load any rows. Errors: {'; '.join(error_summary)}")
            
        return rows_loaded
        
    except Exception as e:
        self.logger.error(f"Loading failed: {e}")
        raise
```

### 3. Configuration

- **Use dataclasses** for type safety and validation
- **Provide sensible defaults** for optional parameters
- **Support environment variables** for sensitive data
- **Validate early** in the constructor

### 4. Connection Management

```python
def connect(self) -> None:
    try:
        self._connection = create_connection(self.config)
        # Always test the connection
        self._connection.ping()
        self.logger.info(f"Connected to {self.config.host}:{self.config.port}")
        self._is_connected = True
    except Exception as e:
        self.logger.error(f"Connection failed: {e}")
        raise

def disconnect(self) -> None:
    if self._connection:
        self._connection.close()
        self._connection = None
    self._is_connected = False
    self.logger.info("Disconnected")
```

## Examples

### Complete PostgreSQL-style Loader

```python
from dataclasses import dataclass
from typing import Any, Dict, Optional
import pyarrow as pa
from ..base import DataLoader, LoadMode

@dataclass
class ExampleConfig:
    host: str
    port: int = 5432
    database: str
    user: str
    password: str
    timeout: int = 30

class ExampleLoader(DataLoader[ExampleConfig]):
    """Complete example loader with all required methods"""
    
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._connection = None
    
    def _parse_config(self, config: Dict[str, Any]) -> ExampleConfig:
        return ExampleConfig(**config)
    
    def _get_required_config_fields(self) -> list[str]:
        return ['host', 'database', 'user', 'password']
    
    def connect(self) -> None:
        try:
            self._connection = create_connection(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                timeout=self.config.timeout
            )
            self._is_connected = True
            self.logger.info(f"Connected to {self.config.host}:{self.config.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise
    
    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False
        self.logger.info("Disconnected")
    
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        # Convert batch to format your system understands
        data_dict = batch.to_pydict()
        
        rows_loaded = 0
        for i in range(batch.num_rows):
            row_data = {col: data_dict[col][i] for col in data_dict.keys()}
            self._connection.insert(table_name, row_data)
            rows_loaded += 1
        
        return rows_loaded
    
    def _get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        return {
            'operation': 'load_batch',
            'batch_size': batch.num_rows,
            'schema_fields': len(batch.schema),
            'throughput_rows_per_sec': round(batch.num_rows / duration, 2) if duration > 0 else 0,
            'host': self.config.host,
            'database': self.config.database
        }
    
    def _get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        return {
            'operation': 'load_table',
            'batch_count': batch_count,
            'batches_processed': batch_count,
            'total_rows': table.num_rows,
            'schema_fields': len(table.schema),
            'avg_batch_size': round(table.num_rows / batch_count, 2) if batch_count > 0 else 0,
            'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
            'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0,
            'host': self.config.host,
            'database': self.config.database
        }
    
    # Optional: Enhanced functionality
    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        columns = []
        for field in schema:
            if pa.types.is_timestamp(field.type):
                sql_type = 'TIMESTAMP'
            elif pa.types.is_int64(field.type):
                sql_type = 'BIGINT'
            elif pa.types.is_string(field.type):
                sql_type = 'VARCHAR'
            else:
                sql_type = 'VARCHAR'  # Safe fallback
            
            nullable = '' if field.nullable else ' NOT NULL'
            columns.append(f'"{field.name}" {sql_type}{nullable}')
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self._connection.execute(sql)
    
    def _clear_table(self, table_name: str) -> None:
        self._connection.execute(f"DELETE FROM {table_name}")
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        try:
            result = self._connection.query(f"SELECT COUNT(*) FROM {table_name}")
            return {
                'table_name': table_name,
                'row_count': result[0]['count'],
                'exists': True
            }
        except Exception:
            return None
```

### Simple Key-Value Loader

```python
@dataclass
class KeyValueConfig:
    host: str
    port: int = 6379
    database: int = 0

class KeyValueLoader(DataLoader[KeyValueConfig]):
    """Simple key-value store loader"""
    
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    
    def _parse_config(self, config: Dict[str, Any]) -> KeyValueConfig:
        return KeyValueConfig(**config)
    
    def _get_required_config_fields(self) -> list[str]:
        return ['host']
    
    def connect(self) -> None:
        self._client = KeyValueClient(
            host=self.config.host,
            port=self.config.port,
            db=self.config.database
        )
        self._is_connected = True
    
    def disconnect(self) -> None:
        if self._client:
            self._client.close()
        self._is_connected = False
    
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        data_dict = batch.to_pydict()
        
        # Assume first column is key
        key_col = batch.schema[0].name
        keys = data_dict[key_col]
        
        rows_loaded = 0
        for i in range(batch.num_rows):
            key = f"{table_name}:{keys[i]}"
            value = {col: data_dict[col][i] for col in data_dict.keys()}
            
            self._client.set(key, value)
            rows_loaded += 1
        
        return rows_loaded
    
    def _get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        return {
            'operation': 'load_batch',
            'batch_size': batch.num_rows,
            'schema_fields': len(batch.schema),
            'throughput_rows_per_sec': round(batch.num_rows / duration, 2) if duration > 0 else 0,
            'host': self.config.host,
            'database': self.config.database
        }
    
    def _get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        return {
            'operation': 'load_table',
            'batch_count': batch_count,
            'batches_processed': batch_count,
            'total_rows': table.num_rows,
            'schema_fields': len(table.schema),
            'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0,
            'host': self.config.host,
            'database': self.config.database
        }
```

This documentation provides everything needed to implement new data loaders efficiently and consistently!