"""
Unit tests for idempotency system.
"""

from datetime import datetime

import pytest

from src.amp.streaming.idempotency import (
    IdempotencyConfig,
    NullProcessedRangesStore,
    ProcessedRange,
)
from src.amp.streaming.types import BlockRange


@pytest.mark.unit
class TestIdempotencyConfig:
    """Test IdempotencyConfig dataclass"""

    def test_default_values(self):
        """Test idempotency config defaults"""
        config = IdempotencyConfig()

        assert config.enabled == False  # Disabled by default
        assert config.table_prefix == 'amp_'
        assert config.verification_hash == False
        assert config.cleanup_days == 30

    def test_custom_values(self):
        """Test idempotency config with custom values"""
        config = IdempotencyConfig(
            enabled=True,
            table_prefix='custom_',
            verification_hash=True,
            cleanup_days=7,
        )

        assert config.enabled == True
        assert config.table_prefix == 'custom_'
        assert config.verification_hash == True
        assert config.cleanup_days == 7


@pytest.mark.unit
class TestProcessedRange:
    """Test ProcessedRange dataclass and methods"""

    def test_creation(self):
        """Test creating a processed range"""
        timestamp = datetime.utcnow()

        pr = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=timestamp,
            batch_hash='abc123',
        )

        assert pr.connection_name == 'test_conn'
        assert pr.table_name == 'test_table'
        assert pr.network == 'ethereum'
        assert pr.start_block == 100
        assert pr.end_block == 200
        assert pr.processed_at == timestamp
        assert pr.batch_hash == 'abc123'

    def test_matches_range_true(self):
        """Test matching a block range"""
        pr = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=datetime.utcnow(),
        )

        block_range = BlockRange(network='ethereum', start=100, end=200)
        assert pr.matches_range(block_range) == True

    def test_matches_range_false_network(self):
        """Test non-matching network"""
        pr = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=datetime.utcnow(),
        )

        block_range = BlockRange(network='polygon', start=100, end=200)
        assert pr.matches_range(block_range) == False

    def test_matches_range_false_blocks(self):
        """Test non-matching block numbers"""
        pr = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=datetime.utcnow(),
        )

        block_range = BlockRange(network='ethereum', start=100, end=300)
        assert pr.matches_range(block_range) == False

    def test_serialization_round_trip(self):
        """Test serialization and deserialization"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0)

        original = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=timestamp,
            batch_hash='abc123',
        )

        # Serialize
        data = original.to_dict()

        # Deserialize
        restored = ProcessedRange.from_dict(data)

        assert restored.connection_name == original.connection_name
        assert restored.table_name == original.table_name
        assert restored.network == original.network
        assert restored.start_block == original.start_block
        assert restored.end_block == original.end_block
        assert restored.processed_at == original.processed_at
        assert restored.batch_hash == original.batch_hash

    def test_to_dict_structure(self):
        """Test dictionary structure"""
        timestamp = datetime(2024, 1, 1, 12, 0, 0)

        pr = ProcessedRange(
            connection_name='test_conn',
            table_name='test_table',
            network='ethereum',
            start_block=100,
            end_block=200,
            processed_at=timestamp,
        )

        data = pr.to_dict()

        assert 'connection_name' in data
        assert 'table_name' in data
        assert 'network' in data
        assert 'start_block' in data
        assert 'end_block' in data
        assert 'processed_at' in data
        assert 'batch_hash' in data

        assert data['connection_name'] == 'test_conn'
        assert data['network'] == 'ethereum'
        assert data['start_block'] == 100
        assert data['processed_at'] == '2024-01-01T12:00:00'


@pytest.mark.unit
class TestNullProcessedRangesStore:
    """Test NullProcessedRangesStore no-op implementation"""

    def test_is_processed_always_false(self):
        """Test that is_processed always returns False"""
        config = IdempotencyConfig(enabled=False)
        store = NullProcessedRangesStore(config)

        ranges = [BlockRange(network='ethereum', start=100, end=200)]
        assert store.is_processed('conn1', 'table1', ranges) == False

    def test_mark_processed_does_nothing(self):
        """Test that mark_processed is a no-op"""
        config = IdempotencyConfig(enabled=False)
        store = NullProcessedRangesStore(config)

        ranges = [BlockRange(network='ethereum', start=100, end=200)]

        # Should not raise
        store.mark_processed('conn1', 'table1', ranges)
        store.mark_processed('conn1', 'table1', ranges, batch_hash='abc123')

    def test_cleanup_does_nothing(self):
        """Test that cleanup returns 0"""
        config = IdempotencyConfig(enabled=False)
        store = NullProcessedRangesStore(config)

        deleted = store.cleanup_old_ranges('conn1', 'table1', 30)
        assert deleted == 0


@pytest.mark.unit
class TestBatchHashing:
    """Test batch content hashing"""

    def test_compute_batch_hash(self):
        """Test computing hash of batch data"""
        from src.amp.streaming.idempotency import compute_batch_hash
        import pyarrow as pa

        # Create test batch
        schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
        batch = pa.record_batch([[1, 2, 3], ['a', 'b', 'c']], schema=schema)

        hash1 = compute_batch_hash(batch)

        assert hash1 is not None
        assert isinstance(hash1, str)
        assert len(hash1) == 64  # SHA256 produces 64-character hex string

    def test_same_data_same_hash(self):
        """Test that same data produces same hash"""
        from src.amp.streaming.idempotency import compute_batch_hash
        import pyarrow as pa

        schema = pa.schema([('id', pa.int64()), ('value', pa.string())])

        batch1 = pa.record_batch([[1, 2, 3], ['a', 'b', 'c']], schema=schema)
        batch2 = pa.record_batch([[1, 2, 3], ['a', 'b', 'c']], schema=schema)

        hash1 = compute_batch_hash(batch1)
        hash2 = compute_batch_hash(batch2)

        assert hash1 == hash2

    def test_different_data_different_hash(self):
        """Test that different data produces different hash"""
        from src.amp.streaming.idempotency import compute_batch_hash
        import pyarrow as pa

        schema = pa.schema([('id', pa.int64()), ('value', pa.string())])

        batch1 = pa.record_batch([[1, 2, 3], ['a', 'b', 'c']], schema=schema)
        batch2 = pa.record_batch([[4, 5, 6], ['d', 'e', 'f']], schema=schema)

        hash1 = compute_batch_hash(batch1)
        hash2 = compute_batch_hash(batch2)

        assert hash1 != hash2
