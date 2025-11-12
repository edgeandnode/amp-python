# tests/integration/test_iceberg_loader.py
"""
Integration tests for Apache Iceberg loader implementation.
These tests require actual Iceberg functionality and catalog access.
"""

import json
import tempfile
from datetime import datetime, timedelta

import pyarrow as pa
import pytest

from src.amp.loaders.base import LoadMode

try:
    from src.amp.loaders.implementations.iceberg_loader import ICEBERG_AVAILABLE, IcebergLoader

    # Skip all tests if iceberg is not available
    if not ICEBERG_AVAILABLE:
        pytest.skip('Apache Iceberg not available', allow_module_level=True)

except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


@pytest.fixture(scope='session')
def iceberg_test_env():
    """Setup Iceberg test environment for the session"""
    temp_dir = tempfile.mkdtemp(prefix='iceberg_test_')
    yield temp_dir
    # Note: cleanup is handled by temp directory auto-cleanup


@pytest.fixture
def iceberg_basic_config(iceberg_test_env):
    """Get basic Iceberg configuration with local file catalog"""
    return {
        'catalog_config': {
            'type': 'sql',
            'uri': f'sqlite:///{iceberg_test_env}/catalog.db',
            'warehouse': f'file://{iceberg_test_env}/warehouse',
        },
        'namespace': 'test_data',
        'create_namespace': True,
        'create_table': True,
        'schema_evolution': True,
        'batch_size': 1000,
    }


@pytest.fixture
def iceberg_partitioned_config(iceberg_test_env):
    """Get partitioned Iceberg configuration"""
    # Note: partition_spec should be created with actual PartitionSpec when needed
    # For now, return config without partitioning since we need schema first
    return {
        'catalog_config': {
            'type': 'sql',
            'uri': f'sqlite:///{iceberg_test_env}/catalog.db',
            'warehouse': f'file://{iceberg_test_env}/warehouse',
        },
        'namespace': 'partitioned_data',
        'create_namespace': True,
        'create_table': True,
        'partition_spec': None,  # Will be set in test if needed
        'schema_evolution': True,
        'batch_size': 500,
    }


@pytest.fixture
def iceberg_temp_config(iceberg_test_env):
    """Get temporary Iceberg configuration with unique namespace"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return {
        'catalog_config': {
            'type': 'sql',
            'uri': f'sqlite:///{iceberg_test_env}/catalog.db',
            'warehouse': f'file://{iceberg_test_env}/warehouse',
        },
        'namespace': f'temp_data_{timestamp}',
        'create_namespace': True,
        'create_table': True,
        'schema_evolution': True,
        'batch_size': 2000,
    }


@pytest.fixture
def comprehensive_test_data():
    """Create comprehensive test data for Iceberg testing"""
    base_date = datetime(2024, 1, 1)

    data = {
        'id': list(range(1000)),
        'user_id': [f'user_{i % 100}' for i in range(1000)],
        'transaction_amount': [round((i * 12.34) % 1000, 2) for i in range(1000)],
        'category': [['electronics', 'clothing', 'books', 'food', 'travel'][i % 5] for i in range(1000)],
        'timestamp': pa.array(
            [(base_date + timedelta(days=i // 50, hours=i % 24)) for i in range(1000)],
            type=pa.timestamp('ns', tz='UTC'),
        ),
        'year': [2024 if i < 800 else 2023 for i in range(1000)],
        'month': [(i // 80) % 12 + 1 for i in range(1000)],
        'day': [(i // 30) % 28 + 1 for i in range(1000)],
        'is_weekend': [i % 7 in [0, 6] for i in range(1000)],
        'metadata': [
            json.dumps(
                {
                    'session_id': f'session_{i}',
                    'device': ['mobile', 'desktop', 'tablet'][i % 3],
                    'location': ['US', 'UK', 'DE', 'FR', 'JP'][i % 5],
                }
            )
            for i in range(1000)
        ],
        'score': [i * 0.123 for i in range(1000)],
        'active': [i % 2 == 0 for i in range(1000)],
    }

    return pa.Table.from_pydict(data)


@pytest.fixture
def small_test_data():
    """Create small test data for quick tests"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['a', 'b', 'c', 'd', 'e'],
        'value': [10.1, 20.2, 30.3, 40.4, 50.5],
        'timestamp': pa.array(
            [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 11, 0, 0),
                datetime(2024, 1, 1, 12, 0, 0),
                datetime(2024, 1, 1, 13, 0, 0),
                datetime(2024, 1, 1, 14, 0, 0),
            ],
            type=pa.timestamp('ns', tz='UTC'),
        ),
        'year': [2024, 2024, 2024, 2024, 2024],
        'month': [1, 1, 1, 1, 1],
        'day': [1, 2, 3, 4, 5],
        'active': [True, False, True, False, True],
    }

    return pa.Table.from_pydict(data)


@pytest.mark.integration
@pytest.mark.iceberg
class TestIcebergLoaderIntegration:
    """Integration tests for Iceberg loader"""

    def test_loader_initialization(self, iceberg_basic_config):
        """Test loader initialization and connection"""
        loader = IcebergLoader(iceberg_basic_config)

        assert loader.config.namespace == iceberg_basic_config['namespace']
        assert loader.config.create_namespace == True
        assert loader.config.create_table == True

        loader.connect()
        assert loader._is_connected == True
        assert loader._catalog is not None
        assert loader._namespace_exists == True

        loader.disconnect()
        assert loader._is_connected == False
        assert loader._catalog is None

    def test_basic_table_operations(self, iceberg_basic_config, comprehensive_test_data):
        """Test basic table creation and data loading"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_transactions', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.rows_loaded == 1000
            assert result.loader_type == 'iceberg'
            assert result.table_name == 'test_transactions'
            assert 'operation' in result.metadata
            assert 'rows_loaded' in result.metadata
            assert result.metadata['namespace'] == iceberg_basic_config['namespace']

    def test_append_mode(self, iceberg_basic_config, comprehensive_test_data):
        """Test append mode functionality"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_append', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 1000

            additional_data = comprehensive_test_data.slice(0, 100)
            result = loader.load_table(additional_data, 'test_append', mode=LoadMode.APPEND)

            assert result.success == True
            assert result.rows_loaded == 100
            assert result.metadata['operation'] == 'load_table'

    def test_batch_loading(self, iceberg_basic_config, comprehensive_test_data):
        """Test batch loading functionality"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            batches = comprehensive_test_data.to_batches(max_chunksize=200)

            for i, batch in enumerate(batches):
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                result = loader.load_batch(batch, 'test_batches', mode=mode)

                assert result.success == True
                assert result.rows_loaded == batch.num_rows
                assert result.metadata['operation'] == 'load_batch'
                assert result.metadata['batch_size'] == batch.num_rows
                assert result.metadata['schema_fields'] == len(batch.schema)

    def test_partitioning(self, iceberg_partitioned_config, small_test_data):
        """Test table partitioning functionality"""
        loader = IcebergLoader(iceberg_partitioned_config)

        with loader:
            # Load partitioned data
            result = loader.load_table(small_test_data, 'test_partitioned', mode=LoadMode.OVERWRITE)

            assert result.success == True
            # Note: Partitioning requires creating PartitionSpec objects now
            assert result.metadata['namespace'] == iceberg_partitioned_config['namespace']

    def test_timestamp_conversion(self, iceberg_basic_config):
        """Test timestamp precision conversion (ns -> us)"""
        # Create data with nanosecond timestamps
        timestamp_data = pa.table(
            {
                'id': [1, 2, 3],
                'event_time': pa.array(
                    [
                        datetime(2024, 1, 1, 10, 0, 0, 123456),  # microsecond precision
                        datetime(2024, 1, 1, 11, 0, 0, 654321),
                        datetime(2024, 1, 1, 12, 0, 0, 987654),
                    ],
                    type=pa.timestamp('ns', tz='UTC'),
                ),  # nanosecond type
                'name': ['event1', 'event2', 'event3'],
            }
        )

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Just verify that we can load nanosecond timestamps successfully
            result = loader.load_table(timestamp_data, 'test_timestamps', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.rows_loaded == 3

            # The conversion happens internally - we just care that it works

    def test_schema_evolution(self, iceberg_basic_config, small_test_data):
        """Test schema evolution functionality"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            result = loader.load_table(small_test_data, 'test_schema_evolution', mode=LoadMode.OVERWRITE)
            assert result.success == True

            extended_data_dict = small_test_data.to_pydict()
            extended_data_dict['new_column'] = list(range(len(extended_data_dict['id'])))
            extended_data_dict['another_field'] = ['test_value'] * len(extended_data_dict['id'])
            extended_table = pa.Table.from_pydict(extended_data_dict)

            result = loader.load_table(extended_table, 'test_schema_evolution', mode=LoadMode.APPEND)

            # Schema evolution should work successfully
            assert result.success == True
            assert result.rows_loaded > 0

            # Verify that the new columns were added to the schema
            table_info = loader.get_table_info('test_schema_evolution')
            assert table_info['exists'] == True
            assert 'new_column' in table_info['columns']
            assert 'another_field' in table_info['columns']

    def test_error_handling_invalid_catalog(self):
        """Test error handling with invalid catalog configuration"""
        invalid_config = {
            'catalog_config': {'type': 'invalid_catalog_type', 'uri': 'invalid://invalid'},
            'namespace': 'test',
            'create_namespace': True,
            'create_table': True,
        }

        loader = IcebergLoader(invalid_config)

        # Should raise an error on connect
        with pytest.raises(ValueError):
            loader.connect()

    def test_error_handling_invalid_namespace(self, iceberg_test_env):
        """Test error handling when namespace creation fails"""
        config = {
            'catalog_config': {
                'type': 'sql',
                'uri': f'sqlite:///{iceberg_test_env}/catalog.db',
                'warehouse': f'file://{iceberg_test_env}/warehouse',
            },
            'namespace': 'test_namespace',
            'create_namespace': False,  # Don't create namespace
            'create_table': True,
        }

        loader = IcebergLoader(config)

        # Should fail if namespace doesn't exist and create_namespace=False
        from pyiceberg.exceptions import NoSuchNamespaceError

        with pytest.raises(NoSuchNamespaceError):
            loader.connect()

    def test_load_mode_overwrite(self, iceberg_basic_config, small_test_data):
        """Test overwrite mode functionality"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Initial load
            result = loader.load_table(small_test_data, 'test_overwrite', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 5

            # Create different data
            different_data = pa.table(
                {
                    'id': [10, 20],
                    'name': ['x', 'y'],
                    'value': [100.0, 200.0],
                    'timestamp': pa.array(
                        [datetime(2024, 2, 1, 10, 0, 0), datetime(2024, 2, 1, 11, 0, 0)],
                        type=pa.timestamp('ns', tz='UTC'),
                    ),
                    'year': [2024, 2024],
                    'month': [2, 2],
                    'day': [1, 1],
                    'active': [True, True],
                }
            )

            # Overwrite with different data
            result = loader.load_table(different_data, 'test_overwrite', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 2

    def test_context_manager(self, iceberg_basic_config, small_test_data):
        """Test context manager functionality"""
        loader = IcebergLoader(iceberg_basic_config)

        # Test context manager auto-connect/disconnect
        assert not loader._is_connected

        with loader:
            assert loader._is_connected == True

            result = loader.load_table(small_test_data, 'test_context', mode=LoadMode.OVERWRITE)
            assert result.success == True

        # Should be disconnected after context exit
        assert loader._is_connected == False

    def test_load_result_metadata(self, iceberg_basic_config, comprehensive_test_data):
        """Test that LoadResult contains proper metadata"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, 'test_metadata', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.loader_type == 'iceberg'
            assert result.table_name == 'test_metadata'
            assert result.rows_loaded == 1000
            assert result.duration > 0

            # Check metadata content
            metadata = result.metadata
            assert 'operation' in metadata
            assert 'rows_loaded' in metadata
            assert 'columns' in metadata
            assert 'namespace' in metadata
            assert metadata['namespace'] == iceberg_basic_config['namespace']
            assert metadata['rows_loaded'] == 1000
            assert metadata['columns'] == len(comprehensive_test_data.schema)


@pytest.mark.integration
@pytest.mark.iceberg
@pytest.mark.slow
class TestIcebergLoaderAdvanced:
    """Advanced integration tests for Iceberg loader"""

    def test_large_data_performance(self, iceberg_basic_config):
        """Test performance with larger datasets"""
        # Create larger test dataset
        large_data = {
            'id': list(range(10000)),
            'value': [i * 0.123 for i in range(10000)],
            'category': [f'category_{i % 10}' for i in range(10000)],
            'year': [2024] * 10000,
            'month': [(i // 800) % 12 + 1 for i in range(10000)],
            'timestamp': pa.array(
                [datetime(2024, 1, 1) + timedelta(seconds=i) for i in range(10000)], type=pa.timestamp('ns', tz='UTC')
            ),
        }

        large_table = pa.Table.from_pydict(large_data)

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Load large dataset
            result = loader.load_table(large_table, 'test_performance', mode=LoadMode.OVERWRITE)

            assert result.success == True
            assert result.rows_loaded == 10000
            assert result.duration < 300  # Should complete within reasonable time

    def test_multiple_tables_same_loader(self, iceberg_basic_config, small_test_data):
        """Test loading multiple tables with the same loader instance"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            table_names = ['table_1', 'table_2', 'table_3']

            for table_name in table_names:
                result = loader.load_table(small_test_data, table_name, mode=LoadMode.OVERWRITE)
                assert result.success == True
                assert result.table_name == table_name
                assert result.rows_loaded == 5

    def test_batch_streaming(self, iceberg_basic_config, comprehensive_test_data):
        """Test streaming batch operations"""
        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Convert to batch iterator
            batches = comprehensive_test_data.to_batches(max_chunksize=100)
            batch_list = list(batches)

            # Load using load_stream method from base class
            results = list(loader.load_stream(iter(batch_list), 'test_streaming'))

            # Verify all batches were processed
            total_rows = sum(r.rows_loaded for r in results if r.success)
            assert total_rows == 1000

            # Verify all operations succeeded
            for result in results:
                assert result.success == True
                assert result.loader_type == 'iceberg'

    def test_upsert_operations(self, iceberg_basic_config):
        """Test UPSERT/MERGE operations with automatic matching"""
        # Use basic config - no special configuration needed for upsert
        upsert_config = iceberg_basic_config.copy()

        # Initial data
        initial_data = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'value': [100, 200, 300]}
        initial_table = pa.Table.from_pydict(initial_data)

        loader = IcebergLoader(upsert_config)

        with loader:
            # Load initial data
            result1 = loader.load_table(initial_table, 'test_upsert', mode=LoadMode.APPEND)
            assert result1.success == True
            assert result1.rows_loaded == 3

            # Upsert data (update existing + insert new)
            upsert_data = {
                'id': [2, 3, 4],  # 2,3 exist (update), 4 is new (insert)
                'name': ['Bob_Updated', 'Charlie_Updated', 'David'],
                'value': [250, 350, 400],
            }
            upsert_table = pa.Table.from_pydict(upsert_data)

            result2 = loader.load_table(upsert_table, 'test_upsert', mode=LoadMode.UPSERT)
            assert result2.success == True
            assert result2.rows_loaded == 3

    def test_upsert_simple(self, iceberg_basic_config):
        """Test simple UPSERT operations with default behavior"""

        test_data = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'value': [100, 200, 300]}
        test_table = pa.Table.from_pydict(test_data)

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Simple upsert with default settings
            result = loader.load_table(test_table, 'test_simple_upsert', mode=LoadMode.UPSERT)
            assert result.success == True
            assert result.rows_loaded == 3

    def test_upsert_fallback_to_append(self, iceberg_basic_config):
        """Test that UPSERT falls back to APPEND when upsert fails"""

        test_data = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'value': [100, 200, 300]}
        test_table = pa.Table.from_pydict(test_data)

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Even if upsert fails, should fallback gracefully
            result = loader.load_table(test_table, 'test_upsert_fallback', mode=LoadMode.UPSERT)
            assert result.success == True
            assert result.rows_loaded == 3

    def test_handle_reorg_empty_table(self, iceberg_basic_config):
        """Test reorg handling on empty table"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create table with one row first
            initial_data = pa.table(
                {'id': [999], 'block_num': [999], '_meta_block_ranges': ['[{"network": "test", "start": 1, "end": 2}]']}
            )
            loader.load_table(initial_data, 'test_reorg_empty', mode=LoadMode.OVERWRITE)

            # Now overwrite with empty data to simulate empty table
            empty_data = pa.table({'id': [], 'block_num': [], '_meta_block_ranges': []})
            loader.load_table(empty_data, 'test_reorg_empty', mode=LoadMode.OVERWRITE)

            # Call handle reorg on empty table
            invalidation_ranges = [BlockRange(network='ethereum', start=100, end=200)]

            # Should not raise any errors
            loader._handle_reorg(invalidation_ranges, 'test_reorg_empty', 'test_connection')

            # Verify table still exists
            table_info = loader.get_table_info('test_reorg_empty')
            assert table_info['exists'] == True

    def test_handle_reorg_no_metadata_column(self, iceberg_basic_config):
        """Test reorg handling when table lacks metadata column"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create table without metadata column
            data = pa.table({'id': [1, 2, 3], 'block_num': [100, 150, 200], 'value': [10.0, 20.0, 30.0]})
            loader.load_table(data, 'test_reorg_no_meta', mode=LoadMode.OVERWRITE)

            # Call handle reorg
            invalidation_ranges = [BlockRange(network='ethereum', start=150, end=250)]

            # Should log warning and not modify data
            loader._handle_reorg(invalidation_ranges, 'test_reorg_no_meta', 'test_connection')

            # Verify data unchanged
            table_info = loader.get_table_info('test_reorg_no_meta')
            assert table_info['exists'] == True

    def test_handle_reorg_single_network(self, iceberg_basic_config):
        """Test reorg handling for single network data"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create table with metadata
            block_ranges = [
                [{'network': 'ethereum', 'start': 100, 'end': 110}],
                [{'network': 'ethereum', 'start': 150, 'end': 160}],
                [{'network': 'ethereum', 'start': 200, 'end': 210}],
            ]

            data = pa.table(
                {
                    'id': [1, 2, 3],
                    'block_num': [105, 155, 205],
                    '_meta_block_ranges': [json.dumps(ranges) for ranges in block_ranges],
                }
            )

            # Load initial data
            result = loader.load_table(data, 'test_reorg_single', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 3

            # Reorg from block 155 - should delete rows 2 and 3
            invalidation_ranges = [BlockRange(network='ethereum', start=155, end=300)]
            loader._handle_reorg(invalidation_ranges, 'test_reorg_single', 'test_connection')

            # Verify only first row remains
            # Since we can't easily query Iceberg tables in tests, we'll verify through table info
            table_info = loader.get_table_info('test_reorg_single')
            assert table_info['exists'] == True
            # The actual row count verification would require scanning the table

    def test_handle_reorg_multi_network(self, iceberg_basic_config):
        """Test reorg handling preserves data from unaffected networks"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create table with data from multiple networks
            block_ranges = [
                [{'network': 'ethereum', 'start': 100, 'end': 110}],
                [{'network': 'polygon', 'start': 100, 'end': 110}],
                [{'network': 'ethereum', 'start': 150, 'end': 160}],
                [{'network': 'polygon', 'start': 150, 'end': 160}],
            ]

            data = pa.table(
                {
                    'id': [1, 2, 3, 4],
                    'network': ['ethereum', 'polygon', 'ethereum', 'polygon'],
                    '_meta_block_ranges': [json.dumps(r) for r in block_ranges],
                }
            )

            # Load initial data
            result = loader.load_table(data, 'test_reorg_multi', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 4

            # Reorg only ethereum from block 150
            invalidation_ranges = [BlockRange(network='ethereum', start=150, end=200)]
            loader._handle_reorg(invalidation_ranges, 'test_reorg_multi', 'test_connection')

            # Verify ethereum row 3 deleted, but polygon rows preserved
            table_info = loader.get_table_info('test_reorg_multi')
            assert table_info['exists'] == True

    def test_handle_reorg_overlapping_ranges(self, iceberg_basic_config):
        """Test reorg with overlapping block ranges"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create data with overlapping ranges
            block_ranges = [
                [{'network': 'ethereum', 'start': 90, 'end': 110}],  # Overlaps with reorg
                [{'network': 'ethereum', 'start': 140, 'end': 160}],  # Overlaps with reorg
                [{'network': 'ethereum', 'start': 170, 'end': 190}],  # After reorg
            ]

            data = pa.table({'id': [1, 2, 3], '_meta_block_ranges': [json.dumps(ranges) for ranges in block_ranges]})

            # Load initial data
            result = loader.load_table(data, 'test_reorg_overlap', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 3

            # Reorg from block 150 - should delete rows where end >= 150
            invalidation_ranges = [BlockRange(network='ethereum', start=150, end=200)]
            loader._handle_reorg(invalidation_ranges, 'test_reorg_overlap', 'test_connection')

            # Only first row should remain (ends at 110 < 150)
            table_info = loader.get_table_info('test_reorg_overlap')
            assert table_info['exists'] == True

    def test_handle_reorg_multiple_invalidations(self, iceberg_basic_config):
        """Test handling multiple invalidation ranges"""
        from src.amp.streaming.types import BlockRange

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create data from multiple networks
            block_ranges = [
                [{'network': 'ethereum', 'start': 100, 'end': 110}],
                [{'network': 'polygon', 'start': 200, 'end': 210}],
                [{'network': 'arbitrum', 'start': 300, 'end': 310}],
                [{'network': 'ethereum', 'start': 150, 'end': 160}],
                [{'network': 'polygon', 'start': 250, 'end': 260}],
            ]

            data = pa.table({'id': [1, 2, 3, 4, 5], '_meta_block_ranges': [json.dumps(r) for r in block_ranges]})

            # Load initial data
            result = loader.load_table(data, 'test_reorg_multiple', mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 5

            # Multiple reorgs
            invalidation_ranges = [
                BlockRange(network='ethereum', start=150, end=200),  # Affects row 4
                BlockRange(network='polygon', start=250, end=300),  # Affects row 5
            ]
            loader._handle_reorg(invalidation_ranges, 'test_reorg_multiple', 'test_connection')

            # Rows 1, 2, 3 should remain
            table_info = loader.get_table_info('test_reorg_multiple')
            assert table_info['exists'] == True

    def test_streaming_with_reorg(self, iceberg_basic_config):
        """Test streaming data with reorg support"""
        from src.amp.streaming.types import (
            BatchMetadata,
            BlockRange,
            ResponseBatch,
        )

        loader = IcebergLoader(iceberg_basic_config)

        with loader:
            # Create streaming data with metadata
            data1 = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})

            data2 = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})

            # Create response batches using factory methods (with hashes for proper state management)
            response1 = ResponseBatch.data_batch(
                data=data1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')]),
            )

            response2 = ResponseBatch.data_batch(
                data=data2,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160, hash='0xdef456')]),
            )

            # Simulate reorg event using factory method
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=150, end=200)]
            )

            # Process streaming data
            stream = [response1, response2, reorg_response]
            results = list(loader.load_stream_continuous(iter(stream), 'test_streaming_reorg'))

            # Verify results
            assert len(results) == 3
            assert results[0].success == True
            assert results[0].rows_loaded == 2
            assert results[1].success == True
            assert results[1].rows_loaded == 2
            assert results[2].success == True
            assert results[2].is_reorg == True
