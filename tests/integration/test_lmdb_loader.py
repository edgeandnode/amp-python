# tests/integration/test_lmdb_loader.py
"""
Integration tests for LMDB loader implementation.
These tests use a local LMDB instance.
"""

import os
import shutil
import tempfile
import time
from datetime import datetime

import pyarrow as pa
import pytest

try:
    from src.amp.loaders.base import LoadMode
    from src.amp.loaders.implementations.lmdb_loader import LMDBLoader
except ImportError:
    pytest.skip('LMD loader modules not available', allow_module_level=True)


@pytest.fixture
def lmdb_test_dir():
    """Create and cleanup temporary directory for LMDB databases"""
    temp_dir = tempfile.mkdtemp(prefix='lmdb_test_')
    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def lmdb_config(lmdb_test_dir):
    """LMDB configuration for testing"""
    return {
        'db_path': os.path.join(lmdb_test_dir, 'test.lmdb'),
        'map_size': 100 * 1024**2,  # 100MB for tests
        'transaction_size': 1000,
    }


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


@pytest.fixture
def sample_test_data():
    """Create sample test data"""
    data = {
        'id': list(range(100)),
        'name': [f'name_{i}' for i in range(100)],
        'value': [i * 1.5 for i in range(100)],
        'active': [i % 2 == 0 for i in range(100)],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def blockchain_test_data():
    """Create blockchain-like test data"""
    data = {
        'block_number': list(range(1000, 2000)),
        'block_hash': [f'0x{i:064x}' for i in range(1000, 2000)],
        'timestamp': [1600000000 + i * 12 for i in range(1000)],
        'transaction_count': [i % 100 + 1 for i in range(1000)],
        'gas_used': [21000 + i * 1000 for i in range(1000)],
    }
    return pa.Table.from_pydict(data)


@pytest.mark.integration
@pytest.mark.lmdb
class TestLMDBLoaderIntegration:
    """Test LMDB loader implementation"""

    def test_connection(self, lmdb_config):
        """Test basic connection to LMDB"""
        loader = LMDBLoader(lmdb_config)

        # Should not be connected initially
        assert not loader.is_connected

        # Connect
        loader.connect()
        assert loader.is_connected
        assert loader.env is not None

        # Check that database was created
        assert os.path.exists(lmdb_config['db_path'])

        # Disconnect
        loader.disconnect()
        assert not loader.is_connected

    def test_load_table_basic(self, lmdb_config, sample_test_data, test_table_name):
        """Test basic table loading"""
        loader = LMDBLoader(lmdb_config)
        loader.connect()

        # Load data
        result = loader.load_table(sample_test_data, test_table_name)

        assert result.success
        assert result.rows_loaded == 100
        assert result.table_name == test_table_name
        assert result.loader_type == 'lmdb'

        # Verify data was stored
        with loader.env.begin() as txn:
            cursor = txn.cursor()
            count = sum(1 for _ in cursor)
            assert count == 100

        loader.disconnect()

    def test_key_column_strategy(self, lmdb_config, sample_test_data, test_table_name):
        """Test key generation using specific column"""
        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        result = loader.load_table(sample_test_data, test_table_name)
        assert result.success

        # Verify keys were generated correctly
        with loader.env.begin() as txn:
            # Check specific keys
            for i in range(10):
                key = str(i).encode('utf-8')
                value = txn.get(key)
                assert value is not None

        loader.disconnect()

    def test_key_pattern_strategy(self, lmdb_config, blockchain_test_data, test_table_name):
        """Test pattern-based key generation"""
        config = {**lmdb_config, 'key_pattern': '{table}:block:{block_number}'}
        loader = LMDBLoader(config)
        loader.connect()

        result = loader.load_table(blockchain_test_data, test_table_name)
        assert result.success

        # Verify pattern-based keys
        with loader.env.begin() as txn:
            key = f'{test_table_name}:block:1000'.encode('utf-8')
            value = txn.get(key)
            assert value is not None

        loader.disconnect()

    def test_composite_key_strategy(self, lmdb_config, sample_test_data, test_table_name):
        """Test composite key generation"""
        config = {**lmdb_config, 'composite_key_columns': ['name', 'id']}
        loader = LMDBLoader(config)
        loader.connect()

        result = loader.load_table(sample_test_data, test_table_name)
        assert result.success

        # Verify composite keys
        with loader.env.begin() as txn:
            key = 'name_0:0'.encode('utf-8')
            value = txn.get(key)
            assert value is not None

        loader.disconnect()

    def test_named_database(self, lmdb_config, sample_test_data):
        """Test using named databases"""
        config = {**lmdb_config, 'database_name': 'blocks', 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Load to named database
        result = loader.load_table(sample_test_data, 'any_table')
        assert result.success

        # Verify data is in named database
        db = loader.env.open_db(b'blocks')
        with loader.env.begin(db=db) as txn:
            cursor = txn.cursor()
            count = sum(1 for _ in cursor)
            assert count == 100

        loader.disconnect()

    def test_load_modes(self, lmdb_config, sample_test_data, test_table_name):
        """Test different load modes"""
        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Initial load
        result1 = loader.load_table(sample_test_data, test_table_name)
        assert result1.success
        assert result1.rows_loaded == 100

        # Append mode (should fail for duplicate keys)
        result2 = loader.load_table(sample_test_data, test_table_name, mode=LoadMode.APPEND)
        assert not result2.success  # Should fail due to duplicate keys
        assert result2.rows_loaded == 0  # No new rows added
        assert 'Key already exists' in str(result2.error)

        # Overwrite mode
        result3 = loader.load_table(sample_test_data, test_table_name, mode=LoadMode.OVERWRITE)
        assert result3.success
        assert result3.rows_loaded == 100

        loader.disconnect()

    def test_transaction_batching(self, lmdb_test_dir, blockchain_test_data, test_table_name):
        """Test transaction batching performance"""
        # Small transactions
        config1 = {
            'db_path': os.path.join(lmdb_test_dir, 'test1.lmdb'),
            'map_size': 100 * 1024**2,
            'transaction_size': 100,
            'key_column': 'block_number',
        }
        loader1 = LMDBLoader(config1)
        loader1.connect()

        start = time.time()
        result1 = loader1.load_table(blockchain_test_data, test_table_name)
        time1 = time.time() - start

        loader1.disconnect()

        # Large transactions - use different database
        config2 = {
            'db_path': os.path.join(lmdb_test_dir, 'test2.lmdb'),
            'map_size': 100 * 1024**2,
            'transaction_size': 1000,
            'key_column': 'block_number',
        }
        loader2 = LMDBLoader(config2)
        loader2.connect()

        start = time.time()
        result2 = loader2.load_table(blockchain_test_data, test_table_name)
        time2 = time.time() - start

        loader2.disconnect()

        # Both should succeed
        assert result1.success
        assert result2.success

        # Larger transactions should generally be faster
        # (though this might not always be true in small test datasets)
        print(f'Small txn time: {time1:.3f}s, Large txn time: {time2:.3f}s')

    def test_byte_key_handling(self, lmdb_config):
        """Test handling of byte array keys"""
        # Create data with byte keys
        data = {'key': [b'key1', b'key2', b'key3'], 'value': [1, 2, 3]}
        table = pa.Table.from_pydict(data)

        config = {**lmdb_config, 'key_column': 'key'}
        loader = LMDBLoader(config)
        loader.connect()

        result = loader.load_table(table, 'byte_test')
        assert result.success
        assert result.rows_loaded == 3

        # Verify byte keys work correctly
        with loader.env.begin() as txn:
            assert txn.get(b'key1') is not None
            assert txn.get(b'key2') is not None
            assert txn.get(b'key3') is not None

        loader.disconnect()

    def test_large_batch_loading(self, lmdb_config):
        """Test loading large batches"""
        # Create larger dataset
        size = 50000
        data = {
            'id': list(range(size)),
            'data': ['x' * 100 for _ in range(size)],  # 100 chars per row
        }
        table = pa.Table.from_pydict(data)

        config = {
            **lmdb_config,
            'map_size': 500 * 1024**2,  # 500MB
            'transaction_size': 5000,
            'key_column': 'id',
        }
        loader = LMDBLoader(config)
        loader.connect()

        start = time.time()
        result = loader.load_table(table, 'large_test')
        duration = time.time() - start

        assert result.success
        assert result.rows_loaded == size

        # Check performance metrics
        throughput = result.metadata['throughput_rows_per_sec']
        print(f'Loaded {size} rows in {duration:.2f}s ({throughput:.0f} rows/sec)')
        assert throughput > 10000  # Should handle at least 10k rows/sec

        loader.disconnect()

    def test_error_handling(self, lmdb_config, sample_test_data):
        """Test error handling"""
        # Test with invalid key column
        config = {**lmdb_config, 'key_column': 'nonexistent_column'}
        loader = LMDBLoader(config)
        loader.connect()

        result = loader.load_table(sample_test_data, 'error_test')
        assert not result.success
        assert 'not found in data' in result.error

        loader.disconnect()

    def test_data_persistence(self, lmdb_config, sample_test_data, test_table_name):
        """Test that data persists across connections"""
        config = {**lmdb_config, 'key_column': 'id'}

        # First connection - write data
        loader1 = LMDBLoader(config)
        loader1.connect()
        result = loader1.load_table(sample_test_data, test_table_name)
        assert result.success
        loader1.disconnect()

        # Second connection - verify data
        loader2 = LMDBLoader(config)
        loader2.connect()

        with loader2.env.begin() as txn:
            # Check a few keys
            for i in range(10):
                key = str(i).encode('utf-8')
                value = txn.get(key)
                assert value is not None

                # Deserialize and verify it's valid Arrow data
                import pyarrow as pa

                reader = pa.ipc.open_stream(value)
                batch = reader.read_next_batch()
                assert batch.num_rows == 1

        loader2.disconnect()

    def test_handle_reorg_empty_db(self, lmdb_config):
        """Test reorg handling on empty database"""
        from src.amp.streaming.types import BlockRange

        loader = LMDBLoader(lmdb_config)
        loader.connect()

        # Call handle reorg on empty database
        invalidation_ranges = [BlockRange(network='ethereum', start=100, end=200)]

        # Should not raise any errors
        loader._handle_reorg(invalidation_ranges, 'test_reorg_empty')

        loader.disconnect()

    def test_handle_reorg_no_metadata(self, lmdb_config):
        """Test reorg handling when data lacks metadata column"""
        from src.amp.streaming.types import BlockRange

        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Create data without metadata column
        data = pa.table({'id': [1, 2, 3], 'block_num': [100, 150, 200], 'value': [10.0, 20.0, 30.0]})
        loader.load_table(data, 'test_reorg_no_meta', mode=LoadMode.OVERWRITE)

        # Call handle reorg
        invalidation_ranges = [BlockRange(network='ethereum', start=150, end=250)]

        # Should not delete any data (no metadata to check)
        loader._handle_reorg(invalidation_ranges, 'test_reorg_no_meta')

        # Verify data still exists
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None
            assert txn.get(b'2') is not None
            assert txn.get(b'3') is not None

        loader.disconnect()

    def test_handle_reorg_single_network(self, lmdb_config):
        """Test reorg handling for single network data"""
        import json

        from src.amp.streaming.types import BlockRange

        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

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
        assert result.success
        assert result.rows_loaded == 3

        # Verify all data exists
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None
            assert txn.get(b'2') is not None
            assert txn.get(b'3') is not None

        # Reorg from block 155 - should delete rows 2 and 3
        invalidation_ranges = [BlockRange(network='ethereum', start=155, end=300)]
        loader._handle_reorg(invalidation_ranges, 'test_reorg_single')

        # Verify only first row remains
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None
            assert txn.get(b'2') is None  # Deleted
            assert txn.get(b'3') is None  # Deleted

        loader.disconnect()

    def test_handle_reorg_multi_network(self, lmdb_config):
        """Test reorg handling preserves data from unaffected networks"""
        import json

        from src.amp.streaming.types import BlockRange

        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Create data from multiple networks
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
        assert result.success
        assert result.rows_loaded == 4

        # Reorg only ethereum from block 150
        invalidation_ranges = [BlockRange(network='ethereum', start=150, end=200)]
        loader._handle_reorg(invalidation_ranges, 'test_reorg_multi')

        # Verify ethereum row 3 deleted, but polygon rows preserved
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None  # ethereum block 100
            assert txn.get(b'2') is not None  # polygon block 100
            assert txn.get(b'3') is None  # ethereum block 150 (deleted)
            assert txn.get(b'4') is not None  # polygon block 150

        loader.disconnect()

    def test_handle_reorg_overlapping_ranges(self, lmdb_config):
        """Test reorg with overlapping block ranges"""
        import json

        from src.amp.streaming.types import BlockRange

        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Create data with overlapping ranges
        block_ranges = [
            [{'network': 'ethereum', 'start': 90, 'end': 110}],  # Overlaps with reorg
            [{'network': 'ethereum', 'start': 140, 'end': 160}],  # Overlaps with reorg
            [{'network': 'ethereum', 'start': 170, 'end': 190}],  # After reorg
        ]

        data = pa.table({'id': [1, 2, 3], '_meta_block_ranges': [json.dumps(ranges) for ranges in block_ranges]})

        # Load initial data
        result = loader.load_table(data, 'test_reorg_overlap', mode=LoadMode.OVERWRITE)
        assert result.success
        assert result.rows_loaded == 3

        # Reorg from block 150 - should delete rows where end >= 150
        invalidation_ranges = [BlockRange(network='ethereum', start=150, end=200)]
        loader._handle_reorg(invalidation_ranges, 'test_reorg_overlap')

        # Only first row should remain (ends at 110 < 150)
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None
            assert txn.get(b'2') is None  # Deleted (end=160 >= 150)
            assert txn.get(b'3') is None  # Deleted (end=190 >= 150)

        loader.disconnect()

    def test_streaming_with_reorg(self, lmdb_config):
        """Test streaming data with reorg support"""
        from src.amp.streaming.types import (
            BatchMetadata,
            BlockRange,
            ResponseBatch,
            ResponseBatchType,
            ResponseBatchWithReorg,
        )

        config = {**lmdb_config, 'key_column': 'id'}
        loader = LMDBLoader(config)
        loader.connect()

        # Create streaming data with metadata
        data1 = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})

        data2 = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})

        # Create response batches
        response1 = ResponseBatchWithReorg(
            batch_type=ResponseBatchType.DATA,
            data=ResponseBatch(
                data=data1, metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110)])
            ),
        )

        response2 = ResponseBatchWithReorg(
            batch_type=ResponseBatchType.DATA,
            data=ResponseBatch(
                data=data2, metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160)])
            ),
        )

        # Simulate reorg event
        reorg_response = ResponseBatchWithReorg(
            batch_type=ResponseBatchType.REORG, invalidation_ranges=[BlockRange(network='ethereum', start=150, end=200)]
        )

        # Process streaming data
        stream = [response1, response2, reorg_response]
        results = list(loader.load_stream_continuous(iter(stream), 'test_streaming_reorg'))

        # Verify results
        assert len(results) == 3
        assert results[0].success
        assert results[0].rows_loaded == 2
        assert results[1].success
        assert results[1].rows_loaded == 2
        assert results[2].success
        assert results[2].is_reorg

        # Verify reorg deleted the second batch
        with loader.env.begin() as txn:
            assert txn.get(b'1') is not None
            assert txn.get(b'2') is not None
            assert txn.get(b'3') is None  # Deleted by reorg
            assert txn.get(b'4') is None  # Deleted by reorg

        loader.disconnect()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
