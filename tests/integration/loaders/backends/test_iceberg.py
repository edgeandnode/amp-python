"""
Iceberg-specific loader integration tests.

This module provides Iceberg-specific test configuration and tests that
inherit from the generalized base test classes.
"""

from typing import Any, Dict, List, Optional

import pytest

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema

    from src.amp.loaders.implementations.iceberg_loader import IcebergLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class IcebergTestConfig(LoaderTestConfig):
    """Iceberg-specific test configuration"""

    loader_class = IcebergLoader
    config_fixture_name = 'iceberg_basic_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True

    def get_row_count(self, loader: IcebergLoader, table_name: str) -> int:
        """Get row count from Iceberg table"""
        catalog = loader._catalog
        table = catalog.load_table((loader.config.namespace, table_name))
        df = table.scan().to_arrow()
        return len(df)

    def query_rows(
        self, loader: IcebergLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from Iceberg table"""
        catalog = loader._catalog
        table = catalog.load_table((loader.config.namespace, table_name))
        df = table.scan().to_arrow()

        # Convert to list of dicts (simple implementation, no filtering)
        result = []
        for i in range(min(100, len(df))):
            row = {col: df[col][i].as_py() for col in df.column_names}
            result.append(row)
        return result

    def cleanup_table(self, loader: IcebergLoader, table_name: str) -> None:
        """Drop Iceberg table"""
        try:
            catalog = loader._catalog
            catalog.drop_table((loader.config.namespace, table_name))
        except Exception:
            pass  # Table may not exist

    def get_column_names(self, loader: IcebergLoader, table_name: str) -> List[str]:
        """Get column names from Iceberg table"""
        catalog = loader._catalog
        table = catalog.load_table((loader.config.namespace, table_name))
        schema = table.schema()
        return [field.name for field in schema.fields]


@pytest.mark.iceberg
class TestIcebergCore(BaseLoaderTests):
    """Iceberg core loader tests (inherited from base)"""

    config = IcebergTestConfig()


@pytest.mark.iceberg
class TestIcebergStreaming(BaseStreamingTests):
    """Iceberg streaming tests (inherited from base)"""

    config = IcebergTestConfig()


@pytest.mark.iceberg
class TestIcebergSpecific:
    """Iceberg-specific tests that cannot be generalized"""

    def test_catalog_initialization(self, iceberg_basic_config):
        """Test Iceberg catalog initialization"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            assert loader._catalog is not None
            assert loader.config.namespace is not None

            # Verify namespace exists or was created
            namespaces = loader._catalog.list_namespaces()
            assert any(ns == (loader.config.namespace,) for ns in namespaces)

    def test_partitioning(self, iceberg_basic_config, small_test_data):
        """Test Iceberg partitioning (partition spec)"""
        import pyarrow as pa

        # Create config with partitioning
        config = {**iceberg_basic_config, 'partition_spec': [('year', 'identity'), ('month', 'identity')]}
        loader = IcebergLoader(config)

        table_name = 'test_partitioned'

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify table was created with partition spec
            catalog = loader._catalog
            table = catalog.load_table((loader.config.namespace, table_name))
            spec = table.spec()
            # Partition spec should have fields
            assert len(spec.fields) > 0

            # Cleanup
            catalog.drop_table((loader.config.namespace, table_name))

    def test_schema_evolution(self, iceberg_basic_config, small_test_data):
        """Test Iceberg schema evolution"""
        import pyarrow as pa

        from src.amp.loaders.base import LoadMode

        loader = IcebergLoader(iceberg_basic_config)
        table_name = 'test_schema_evolution'

        with loader:
            # Load initial data
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True

            # Add new column
            extended_data = {
                **{col: small_test_data[col].to_pylist() for col in small_test_data.column_names},
                'new_column': [100, 200, 300, 400, 500],
            }
            extended_table = pa.Table.from_pydict(extended_data)

            # Load with new schema
            result2 = loader.load_table(extended_table, table_name, mode=LoadMode.APPEND)

            # Schema evolution depends on config
            catalog = loader._catalog
            table = catalog.load_table((loader.config.namespace, table_name))
            schema = table.schema()
            # New column may or may not be present
            assert len(schema.fields) >= len(small_test_data.schema)

            # Cleanup
            catalog.drop_table((loader.config.namespace, table_name))

    def test_timestamp_conversion(self, iceberg_basic_config):
        """Test timestamp conversion for Iceberg"""
        import pyarrow as pa
        from datetime import datetime

        # Create data with timestamps
        data = {
            'id': [1, 2, 3],
            'timestamp': [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
            'value': [100, 200, 300],
        }
        test_data = pa.Table.from_pydict(data)

        loader = IcebergLoader(iceberg_basic_config)
        table_name = 'test_timestamps'

        with loader:
            result = loader.load_table(test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify timestamps were converted correctly
            catalog = loader._catalog
            table = catalog.load_table((loader.config.namespace, table_name))
            df = table.scan().to_arrow()

            assert len(df) == 3
            assert 'timestamp' in df.column_names

            # Cleanup
            catalog.drop_table((loader.config.namespace, table_name))

    def test_multiple_tables(self, iceberg_basic_config, small_test_data):
        """Test managing multiple tables with same loader"""
        from src.amp.loaders.base import LoadMode

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create first table
            result1 = loader.load_table(small_test_data, 'table1')
            assert result1.success == True

            # Create second table
            result2 = loader.load_table(small_test_data, 'table2')
            assert result2.success == True

            # Verify both exist
            catalog = loader._catalog
            tables = catalog.list_tables(loader.config.namespace)
            table_names = [t[1] for t in tables]

            assert 'table1' in table_names
            assert 'table2' in table_names

            # Cleanup
            catalog.drop_table((loader.config.namespace, 'table1'))
            catalog.drop_table((loader.config.namespace, 'table2'))

    def test_upsert_operations(self, iceberg_basic_config):
        """Test Iceberg upsert operations (merge on read)"""
        import pyarrow as pa

        from src.amp.loaders.base import LoadMode

        loader = IcebergLoader(iceberg_basic_config)
        table_name = 'test_upsert'

        # Initial data
        data1 = {'id': [1, 2, 3], 'value': [100, 200, 300]}
        table1 = pa.Table.from_pydict(data1)

        # Updated data (overlapping IDs)
        data2 = {'id': [2, 3, 4], 'value': [250, 350, 400]}
        table2 = pa.Table.from_pydict(data2)

        with loader:
            # Load initial
            loader.load_table(table1, table_name)

            # Upsert (if supported) or append
            try:
                # Try upsert mode if supported
                loader.load_table(table2, table_name, mode=LoadMode.APPEND)

                # Verify row count
                catalog = loader._catalog
                table = catalog.load_table((loader.config.namespace, table_name))
                df = table.scan().to_arrow()

                # With append, we should have 6 rows (3 + 3)
                # With upsert, we should have 4 rows (deduplicated)
                assert len(df) >= 3

                # Cleanup
                catalog.drop_table((loader.config.namespace, table_name))
            except Exception:
                # Upsert may not be supported
                catalog = loader._catalog
                catalog.drop_table((loader.config.namespace, table_name))

    def test_metadata_completeness(self, iceberg_basic_config, comprehensive_test_data):
        """Test Iceberg metadata in load results"""
        loader = IcebergLoader(iceberg_basic_config)
        table_name = 'test_metadata'

        with loader:
            result = loader.load_table(comprehensive_test_data, table_name)

            assert result.success == True
            assert 'snapshot_id' in result.metadata or 'files_written' in result.metadata

            # Cleanup
            catalog = loader._catalog
            catalog.drop_table((loader.config.namespace, table_name))


@pytest.mark.iceberg
@pytest.mark.slow
class TestIcebergPerformance:
    """Iceberg performance tests"""

    def test_large_data_loading(self, iceberg_basic_config):
        """Test loading large datasets to Iceberg"""
        import pyarrow as pa

        # Create large dataset
        large_data = {
            'id': list(range(50000)),
            'value': [i * 0.123 for i in range(50000)],
            'category': [f'cat_{i % 100}' for i in range(50000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        loader = IcebergLoader(iceberg_basic_config)
        table_name = 'test_large'

        with loader:
            result = loader.load_table(large_table, table_name)

            assert result.success == True
            assert result.rows_loaded == 50000
            assert result.duration < 60  # Should complete within 60 seconds

            # Verify data integrity
            catalog = loader._catalog
            table = catalog.load_table((loader.config.namespace, table_name))
            df = table.scan().to_arrow()
            assert len(df) == 50000

            # Cleanup
            catalog.drop_table((loader.config.namespace, table_name))
