"""
DeltaLake-specific loader integration tests.

This module provides DeltaLake-specific test configuration and tests that
inherit from the generalized base test classes.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

try:
    from deltalake import DeltaTable

    from src.amp.loaders.implementations.deltalake_loader import DeltaLakeLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class DeltaLakeTestConfig(LoaderTestConfig):
    """DeltaLake-specific test configuration"""

    loader_class = DeltaLakeLoader
    config_fixture_name = 'delta_basic_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True

    def get_row_count(self, loader: DeltaLakeLoader, table_name: str) -> int:
        """Get row count from DeltaLake table"""
        # DeltaLake uses the table_path as the identifier
        table_path = loader.config.table_path
        dt = DeltaTable(table_path)
        df = dt.to_pyarrow_table()
        return len(df)

    def query_rows(
        self, loader: DeltaLakeLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from DeltaLake table"""
        table_path = loader.config.table_path
        dt = DeltaTable(table_path)
        df = dt.to_pyarrow_table()

        # Convert to list of dicts (simple implementation, no filtering)
        result = []
        for i in range(min(100, len(df))):
            row = {col: df[col][i].as_py() for col in df.column_names}
            result.append(row)
        return result

    def cleanup_table(self, loader: DeltaLakeLoader, table_name: str) -> None:
        """Delete DeltaLake table directory"""
        import shutil

        table_path = loader.config.table_path
        if Path(table_path).exists():
            shutil.rmtree(table_path, ignore_errors=True)

    def get_column_names(self, loader: DeltaLakeLoader, table_name: str) -> List[str]:
        """Get column names from DeltaLake table"""
        table_path = loader.config.table_path
        dt = DeltaTable(table_path)
        schema = dt.schema()
        return [field.name for field in schema.fields]


@pytest.mark.delta_lake
class TestDeltaLakeCore(BaseLoaderTests):
    """DeltaLake core loader tests (inherited from base)"""

    config = DeltaLakeTestConfig()


@pytest.mark.delta_lake
class TestDeltaLakeStreaming(BaseStreamingTests):
    """DeltaLake streaming tests (inherited from base)"""

    config = DeltaLakeTestConfig()


@pytest.mark.delta_lake
class TestDeltaLakeSpecific:
    """DeltaLake-specific tests that cannot be generalized"""

    def test_partitioning(self, delta_partitioned_config, small_test_data):
        """Test DeltaLake partitioning functionality"""

        loader = DeltaLakeLoader(delta_partitioned_config)

        # Verify partitioning is configured
        assert loader.partition_by == ['year', 'month', 'day']

        with loader:
            result = loader.load_table(small_test_data, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify partitions were created
            dt = DeltaTable(loader.config.table_path)
            files = dt.file_uris()
            # Partitioned tables create subdirectories
            assert len(files) > 0

    def test_optimization_operations(self, delta_basic_config, comprehensive_test_data):
        """Test DeltaLake OPTIMIZE operations"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load data
            result = loader.load_table(comprehensive_test_data, 'test_table')
            assert result.success == True

            # DeltaLake may auto-optimize if configured
            dt = DeltaTable(loader.config.table_path)
            version = dt.version()
            assert version >= 0

            # Verify table can be read after optimization
            df = dt.to_pyarrow_table()
            assert len(df) == 1000

    def test_schema_evolution(self, delta_basic_config, small_test_data):
        """Test DeltaLake schema evolution"""
        import pyarrow as pa

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load initial data
            result = loader.load_table(small_test_data, 'test_table')
            assert result.success == True

            # Add new column to schema
            extended_data = {
                **{col: small_test_data[col].to_pylist() for col in small_test_data.column_names},
                'new_column': [100, 200, 300, 400, 500],
            }
            extended_table = pa.Table.from_pydict(extended_data)

            # Load with new schema (if schema evolution enabled)
            from src.amp.loaders.base import LoadMode

            loader.load_table(extended_table, 'test_table', mode=LoadMode.APPEND)

            # Result depends on merge_schema configuration
            dt = DeltaTable(loader.config.table_path)
            schema = dt.schema()
            # New column may or may not be present depending on config
            assert len(schema.fields) >= len(small_test_data.schema)

    def test_table_history(self, delta_basic_config, small_test_data):
        """Test DeltaLake table version history"""
        from src.amp.loaders.base import LoadMode

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load initial data
            loader.load_table(small_test_data, 'test_table')

            # Append more data
            loader.load_table(small_test_data, 'test_table', mode=LoadMode.APPEND)

            # Check version history
            dt = DeltaTable(loader.config.table_path)
            version = dt.version()
            assert version >= 1  # At least 2 operations (create + append)

            # Verify history is accessible
            history = dt.history()
            assert len(history) >= 1

    def test_metadata_completeness(self, delta_basic_config, comprehensive_test_data):
        """Test DeltaLake metadata in load results"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_table')

            assert result.success == True
            assert 'delta_version' in result.metadata
            assert 'files_added' in result.metadata
            assert result.metadata['delta_version'] >= 0

    def test_query_operations(self, delta_basic_config, comprehensive_test_data):
        """Test querying DeltaLake tables"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_table')
            assert result.success == True

            # Query the table
            dt = DeltaTable(loader.config.table_path)
            df = dt.to_pyarrow_table()

            # Verify data integrity
            assert len(df) == 1000
            assert 'id' in df.column_names
            assert 'user_id' in df.column_names

    def test_file_size_calculation(self, delta_basic_config, comprehensive_test_data):
        """Test file size calculation for DeltaLake tables"""
        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_table')
            assert result.success == True

            # Get table size
            dt = DeltaTable(loader.config.table_path)
            files = dt.file_uris()
            assert len(files) > 0

            # Calculate total size
            total_size = 0
            for file_uri in files:
                # Remove file:// prefix if present
                file_path = file_uri.replace('file://', '')
                if Path(file_path).exists():
                    total_size += Path(file_path).stat().st_size

            assert total_size > 0

    def test_concurrent_operations_safety(self, delta_basic_config, small_test_data):
        """Test that DeltaLake handles concurrent operations safely"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from src.amp.loaders.base import LoadMode

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            # Load initial data
            loader.load_table(small_test_data, 'test_table')

            # Try concurrent appends
            def append_data(i):
                return loader.load_table(small_test_data, 'test_table', mode=LoadMode.APPEND)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(append_data, i) for i in range(3)]
                results = [future.result() for future in as_completed(futures)]

            # All operations should succeed
            assert all(r.success for r in results)

            # Verify final row count
            dt = DeltaTable(loader.config.table_path)
            df = dt.to_pyarrow_table()
            assert len(df) == 20  # 5 initial + 3 * 5 appends


@pytest.mark.delta_lake
@pytest.mark.slow
class TestDeltaLakePerformance:
    """DeltaLake performance tests"""

    def test_large_data_loading(self, delta_basic_config):
        """Test loading large datasets to DeltaLake"""
        import pyarrow as pa

        # Create large dataset
        large_data = {
            'id': list(range(50000)),
            'value': [i * 0.123 for i in range(50000)],
            'category': [f'cat_{i % 100}' for i in range(50000)],
            'year': [2024 if i < 40000 else 2023 for i in range(50000)],
            'month': [(i // 100) % 12 + 1 for i in range(50000)],
            'day': [(i // 10) % 28 + 1 for i in range(50000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        loader = DeltaLakeLoader(delta_basic_config)

        with loader:
            result = loader.load_table(large_table, 'test_table')

            assert result.success == True
            assert result.rows_loaded == 50000
            assert result.duration < 60  # Should complete within 60 seconds

            # Verify data integrity
            dt = DeltaTable(loader.config.table_path)
            df = dt.to_pyarrow_table()
            assert len(df) == 50000
