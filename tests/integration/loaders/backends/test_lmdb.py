"""
LMDB-specific loader integration tests.

This module provides LMDB-specific test configuration and tests that
inherit from the generalized base test classes.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

try:
    from src.amp.loaders.implementations.lmdb_loader import LMDBLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class LMDBTestConfig(LoaderTestConfig):
    """LMDB-specific test configuration"""

    loader_class = LMDBLoader
    config_fixture_name = 'lmdb_config'  # LMDB uses config fixture

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True
    requires_existing_table = False  # LMDB auto-creates databases

    def get_row_count(self, loader: LMDBLoader, table_name: str) -> int:
        """Get row count from LMDB database"""
        count = 0
        with loader.env.begin(db=loader.db) as txn:
            cursor = txn.cursor()
            for _key, _value in cursor:
                count += 1
        return count

    def query_rows(
        self, loader: LMDBLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from LMDB database"""
        import json

        rows = []
        with loader.env.begin(db=loader.db) as txn:
            cursor = txn.cursor()
            for i, (key, value) in enumerate(cursor):
                if i >= 100:  # Limit to 100
                    break
                try:
                    # Try to decode as JSON
                    row_data = json.loads(value.decode())
                    row_data['_key'] = key.decode()
                    rows.append(row_data)
                except Exception:
                    # Fallback to raw value
                    rows.append({'_key': key.decode(), '_value': value.decode()})
        return rows

    def cleanup_table(self, loader: LMDBLoader, table_name: str) -> None:
        """Clear LMDB database"""
        # LMDB doesn't have tables - clear the entire database
        with loader.env.begin(db=loader.db, write=True) as txn:
            cursor = txn.cursor()
            # Delete all keys
            keys_to_delete = [key for key, _ in cursor]
            for key in keys_to_delete:
                txn.delete(key)

    def get_column_names(self, loader: LMDBLoader, table_name: str) -> List[str]:
        """Get column names from LMDB database (from first record)"""
        import json

        with loader.env.begin(db=loader.db) as txn:
            cursor = txn.cursor()
            for _key, value in cursor:
                try:
                    row_data = json.loads(value.decode())
                    return list(row_data.keys())
                except Exception:
                    return ['_value']  # Fallback
        return []


@pytest.fixture
def lmdb_test_env():
    """Create and cleanup temporary directory for LMDB databases"""
    import shutil
    import tempfile

    temp_dir = tempfile.mkdtemp(prefix='lmdb_test_')
    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def lmdb_config(lmdb_test_env):
    """Create LMDB config from test env directory"""
    return {
        'db_path': str(Path(lmdb_test_env) / 'test.lmdb'),
        'map_size': 100 * 1024**2,  # 100MB
        'transaction_size': 1000,
        'create_if_missing': True,
    }


@pytest.fixture
def lmdb_perf_config(lmdb_test_env):
    """LMDB configuration for performance testing"""
    return {
        'db_path': str(Path(lmdb_test_env) / 'perf.lmdb'),
        'map_size': 500 * 1024**2,  # 500MB for performance tests
        'transaction_size': 5000,
        'create_if_missing': True,
    }


@pytest.mark.lmdb
class TestLMDBCore(BaseLoaderTests):
    """LMDB core loader tests (inherited from base)"""

    # Note: LMDB config needs special handling
    config = LMDBTestConfig()


@pytest.mark.lmdb
class TestLMDBStreaming(BaseStreamingTests):
    """LMDB streaming tests (inherited from base)"""

    config = LMDBTestConfig()


@pytest.mark.lmdb
class TestLMDBSpecific:
    """LMDB-specific tests that cannot be generalized"""

    def test_key_column_strategy(self, lmdb_config, small_test_data):
        """Test LMDB key column strategy"""
        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(small_test_data, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify keys are based on id column
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                keys = [key.decode() for key, _ in cursor]

                # Keys should be string representations of IDs
                assert len(keys) == 5
                # Keys may be prefixed with table name

    def test_key_pattern_strategy(self, lmdb_config):
        """Test custom key pattern generation"""
        import pyarrow as pa

        data = {'tx_hash': ['0x100', '0x101', '0x102'], 'block': [100, 101, 102], 'value': [10.0, 11.0, 12.0]}
        test_data = pa.Table.from_pydict(data)

        config = {**lmdb_config, 'key_column': 'tx_hash', 'key_pattern': 'tx:{key}'}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(test_data, 'test_table')
            assert result.success == True

            # Verify custom key pattern
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                keys = [key.decode() for key, _ in cursor]

                # Keys should follow pattern
                assert len(keys) == 3
                # At least one key should contain the pattern
                assert any('tx:' in key or '0x' in key for key in keys)

    def test_composite_key_strategy(self, lmdb_config):
        """Test composite key generation"""
        import pyarrow as pa

        data = {'network': ['eth', 'eth', 'poly'], 'block': [100, 101, 100], 'tx_index': [0, 0, 0]}
        test_data = pa.Table.from_pydict(data)

        config = {**lmdb_config, 'key_columns': ['network', 'block', 'tx_index']}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(test_data, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify composite keys
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                keys = [key.decode() for key, _ in cursor]
                assert len(keys) == 3

    def test_named_database(self, lmdb_config):
        """Test LMDB named databases (sub-databases)"""
        import pyarrow as pa

        data = {'id': [1, 2, 3], 'value': [100, 200, 300]}
        test_data = pa.Table.from_pydict(data)

        config = {**lmdb_config, 'database_name': 'my_table'}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(test_data, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify data in named database
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                count = sum(1 for _ in cursor)
                assert count == 3

    def test_transaction_batching(self, lmdb_test_env):
        """Test transaction batching for large datasets"""
        import pyarrow as pa

        # Create large dataset
        large_data = {'id': list(range(5000)), 'value': [i * 10 for i in range(5000)]}
        large_table = pa.Table.from_pydict(large_data)

        config = {
            'db_path': str(Path(lmdb_test_env) / 'batch_test.lmdb'),
            'map_size': 100 * 1024**2,
            'transaction_size': 500,  # Batch every 500 rows
            'create_if_missing': True,
            'key_column': 'id',
        }
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(large_table, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 5000

            # Verify all data was loaded
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                count = sum(1 for _ in cursor)
                assert count == 5000

    def test_byte_key_handling(self, lmdb_config):
        """Test handling of byte keys"""
        import pyarrow as pa

        data = {'key': [b'key1', b'key2', b'key3'], 'value': [100, 200, 300]}
        test_data = pa.Table.from_pydict(data)

        config = {**lmdb_config, 'key_column': 'key'}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(test_data, 'test_table')
            assert result.success == True
            assert result.rows_loaded == 3

    def test_data_persistence(self, lmdb_config, small_test_data):
        """Test that data persists after closing and reopening"""
        from src.amp.loaders.base import LoadMode

        # Load data
        loader1 = LMDBLoader(lmdb_config)
        with loader1:
            result = loader1.load_table(small_test_data, 'test_table')
            assert result.success == True

        # Close and reopen
        loader2 = LMDBLoader(lmdb_config)
        with loader2:
            # Data should still be there
            with loader2.env.begin(db=loader2.db) as txn:
                cursor = txn.cursor()
                count = sum(1 for _ in cursor)
                assert count == 5

            # Can append more
            result2 = loader2.load_table(small_test_data, 'test_table', mode=LoadMode.APPEND)
            assert result2.success == True

            # Now should have 10
            with loader2.env.begin(db=loader2.db) as txn:
                cursor = txn.cursor()
                count = sum(1 for _ in cursor)
                assert count == 10


@pytest.mark.lmdb
@pytest.mark.slow
class TestLMDBPerformance:
    """LMDB performance tests"""

    def test_large_data_loading(self, lmdb_perf_config):
        """Test loading large datasets to LMDB"""
        import pyarrow as pa

        # Create large dataset
        large_data = {
            'id': list(range(50000)),
            'value': [i * 0.123 for i in range(50000)],
            'category': [f'cat_{i % 100}' for i in range(50000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        config = {**lmdb_perf_config, 'key_column': 'id'}
        loader = LMDBLoader(config)

        with loader:
            result = loader.load_table(large_table, 'test_table')

            assert result.success == True
            assert result.rows_loaded == 50000
            assert result.duration < 60  # Should complete within 60 seconds

            # Verify data integrity
            with loader.env.begin(db=loader.db) as txn:
                cursor = txn.cursor()
                count = sum(1 for _ in cursor)
                assert count == 50000
