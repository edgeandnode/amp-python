"""
ClickHouse-specific loader integration tests.

This module provides ClickHouse-specific test configuration and tests that
inherit from the generalized base test classes.
"""

import time
from typing import Any, Dict, List, Optional

import pytest

try:
    from src.amp.loaders.implementations.clickhouse_loader import ClickHouseLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class ClickHouseTestConfig(LoaderTestConfig):
    """ClickHouse-specific test configuration"""

    loader_class = ClickHouseLoader
    config_fixture_name = 'clickhouse_test_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True
    requires_existing_table = True

    def get_row_count(self, loader: ClickHouseLoader, table_name: str) -> int:
        """Get row count from ClickHouse table"""
        return loader.get_row_count(table_name)

    def query_rows(
        self, loader: ClickHouseLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from ClickHouse table"""
        query = f'SELECT * FROM {table_name}'
        if where:
            query += f' WHERE {where}'
        if order_by:
            query += f' ORDER BY {order_by}'

        result = loader._client.query(query)

        # Convert to list of dicts
        column_names = result.column_names
        rows = []
        for row in result.result_rows:
            rows.append(dict(zip(column_names, row, strict=False)))
        return rows

    def cleanup_table(self, loader: ClickHouseLoader, table_name: str) -> None:
        """Drop ClickHouse table"""
        try:
            loader._client.command(f'DROP TABLE IF EXISTS {table_name}')
        except Exception:
            pass

    def get_column_names(self, loader: ClickHouseLoader, table_name: str) -> List[str]:
        """Get column names from ClickHouse table"""
        result = loader._client.query(f'DESCRIBE TABLE {table_name}')
        return [row[0] for row in result.result_rows]


@pytest.mark.clickhouse
class TestClickHouseCore(BaseLoaderTests):
    """ClickHouse core loader tests (inherited from base)"""

    config = ClickHouseTestConfig()


@pytest.mark.clickhouse
class TestClickHouseStreaming(BaseStreamingTests):
    """ClickHouse streaming tests (inherited from base)"""

    config = ClickHouseTestConfig()


@pytest.fixture
def cleanup_tables(clickhouse_test_config):
    """Cleanup test tables after tests"""
    tables_to_clean = []

    yield tables_to_clean

    # Cleanup
    loader = ClickHouseLoader(clickhouse_test_config)
    try:
        loader.connect()
        for table in tables_to_clean:
            try:
                loader._client.command(f'DROP TABLE IF EXISTS {table}')
            except Exception:
                pass
        loader.disconnect()
    except Exception:
        pass


@pytest.mark.clickhouse
class TestClickHouseSpecific:
    """ClickHouse-specific tests that cannot be generalized"""

    def test_large_batch_loading(self, clickhouse_test_config, medium_test_table, test_table_name, cleanup_tables):
        """Test ClickHouse's ability to handle large batches efficiently"""
        cleanup_tables.append(test_table_name)

        loader = ClickHouseLoader(clickhouse_test_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(medium_test_table, test_table_name)
            duration = time.time() - start_time

            assert result.success == True
            assert result.rows_loaded == 10000
            # ClickHouse should handle 10k rows very quickly
            assert duration < 30  # Should complete within 30 seconds

            # Verify data integrity
            row_count = loader.get_row_count(test_table_name)
            assert row_count == 10000

    def test_schema_retrieval(self, clickhouse_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test schema retrieval functionality"""
        cleanup_tables.append(test_table_name)

        loader = ClickHouseLoader(clickhouse_test_config)

        with loader:
            # Create table
            result = loader.load_table(small_test_data, test_table_name)
            assert result.success == True

            # Get schema
            schema = loader.get_table_schema(test_table_name)
            assert schema is not None

            # Filter out metadata columns added by ClickHouse loader
            non_meta_fields = [
                field for field in schema if not (field.name.startswith('_meta_') or field.name.startswith('_amp_'))
            ]

            assert len(non_meta_fields) == len(small_test_data.schema)

            # Verify column names match (excluding metadata columns)
            original_names = set(small_test_data.schema.names)
            retrieved_names = set(field.name for field in non_meta_fields)
            assert original_names == retrieved_names

    def test_health_check(self, clickhouse_test_config):
        """Test health check functionality"""
        loader = ClickHouseLoader(clickhouse_test_config)

        # Test health check before connection
        health = loader.health_check()
        assert health['healthy'] == False

        # Test health check after connection
        with loader:
            health = loader.health_check()
            assert health['healthy'] == True
            assert 'server_version' in health
            assert health['database'] == clickhouse_test_config.get('database', 'default')

    def test_table_exists(self, clickhouse_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test table existence check"""
        cleanup_tables.append(test_table_name)

        loader = ClickHouseLoader(clickhouse_test_config)

        with loader:
            # Table shouldn't exist yet
            assert loader.table_exists(test_table_name) == False

            # Load data to create table
            result = loader.load_table(small_test_data, test_table_name)
            assert result.success == True

            # Table should now exist
            assert loader.table_exists(test_table_name) == True

    def test_arrow_type_mapping(self, clickhouse_test_config, test_table_name, cleanup_tables):
        """Test Arrow to ClickHouse type mapping"""
        import pyarrow as pa

        cleanup_tables.append(test_table_name)

        # Create data with various types
        data = {
            'int8_col': pa.array([1, 2, 3], type=pa.int8()),
            'int16_col': pa.array([1, 2, 3], type=pa.int16()),
            'int32_col': pa.array([1, 2, 3], type=pa.int32()),
            'int64_col': pa.array([1, 2, 3], type=pa.int64()),
            'uint8_col': pa.array([1, 2, 3], type=pa.uint8()),
            'uint16_col': pa.array([1, 2, 3], type=pa.uint16()),
            'uint32_col': pa.array([1, 2, 3], type=pa.uint32()),
            'uint64_col': pa.array([1, 2, 3], type=pa.uint64()),
            'float32_col': pa.array([1.0, 2.0, 3.0], type=pa.float32()),
            'float64_col': pa.array([1.0, 2.0, 3.0], type=pa.float64()),
            'string_col': pa.array(['a', 'b', 'c'], type=pa.string()),
            'bool_col': pa.array([True, False, True], type=pa.bool_()),
        }
        table = pa.Table.from_pydict(data)

        loader = ClickHouseLoader(clickhouse_test_config)

        with loader:
            result = loader.load_table(table, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify data types were mapped correctly
            schema = loader.get_table_schema(test_table_name)
            assert schema is not None

            # Verify data integrity
            row_count = loader.get_row_count(test_table_name)
            assert row_count == 3


@pytest.mark.clickhouse
@pytest.mark.slow
class TestClickHousePerformance:
    """ClickHouse performance tests"""

    def test_large_data_loading(self, clickhouse_test_config, test_table_name, cleanup_tables):
        """Test loading large datasets"""
        import pyarrow as pa

        cleanup_tables.append(test_table_name)

        # Create large dataset - ClickHouse handles large batches well
        large_data = {
            'id': list(range(100000)),
            'value': [i * 0.123 for i in range(100000)],
            'category': [f'category_{i % 100}' for i in range(100000)],
            'description': [f'Description for row {i}' for i in range(100000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        loader = ClickHouseLoader(clickhouse_test_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(large_table, test_table_name)
            duration = time.time() - start_time

            assert result.success == True
            assert result.rows_loaded == 100000
            assert duration < 120  # Should complete within 2 minutes

            # Log throughput for performance tracking
            throughput = result.rows_loaded / duration
            print(f'\nClickHouse throughput: {throughput:.0f} rows/second')

            # Verify data integrity
            row_count = loader.get_row_count(test_table_name)
            assert row_count == 100000
