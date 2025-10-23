"""
Unit tests for checkpoint system.
"""

from datetime import datetime

import pytest

from src.amp.streaming.checkpoint import CheckpointConfig, CheckpointState, NullCheckpointStore
from src.amp.streaming.types import BlockRange


@pytest.mark.unit
class TestCheckpointState:
    """Test CheckpointState dataclass and methods"""

    def test_creation(self):
        """Test creating a checkpoint state"""
        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc'),
            BlockRange(network='polygon', start=50, end=150, hash='0xdef'),
        ]
        timestamp = datetime.utcnow()

        checkpoint = CheckpointState(
            ranges=ranges,
            timestamp=timestamp,
            worker_id=0,
        )

        assert len(checkpoint.ranges) == 2
        assert checkpoint.timestamp == timestamp
        assert checkpoint.worker_id == 0

    def test_to_resume_watermark(self):
        """Test converting checkpoint to resume watermark"""
        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc'),
        ]
        timestamp = datetime(2024, 1, 1, 0, 0, 0)

        checkpoint = CheckpointState(
            ranges=ranges,
            timestamp=timestamp,
        )

        watermark = checkpoint.to_resume_watermark()

        assert len(watermark.ranges) == 1
        assert watermark.ranges[0]['network'] == 'ethereum'
        assert watermark.ranges[0]['start'] == 100
        assert watermark.ranges[0]['end'] == 200
        assert watermark.timestamp == '2024-01-01T00:00:00'

    def test_serialization_round_trip(self):
        """Test checkpoint serialization and deserialization"""
        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc', prev_hash='0x123'),
            BlockRange(network='polygon', start=50, end=150, hash='0xdef'),
        ]
        timestamp = datetime(2024, 1, 1, 12, 30, 0)

        original = CheckpointState(
            ranges=ranges,
            timestamp=timestamp,
            worker_id=1,
        )

        # Serialize
        data = original.to_dict()

        # Deserialize
        restored = CheckpointState.from_dict(data)

        assert len(restored.ranges) == 2
        assert restored.ranges[0].network == 'ethereum'
        assert restored.ranges[0].hash == '0xabc'
        assert restored.ranges[0].prev_hash == '0x123'
        assert restored.ranges[1].network == 'polygon'
        assert restored.timestamp == timestamp
        assert restored.worker_id == 1

    def test_to_dict_structure(self):
        """Test checkpoint dictionary structure"""
        ranges = [BlockRange(network='ethereum', start=100, end=200)]
        timestamp = datetime(2024, 1, 1, 0, 0, 0)

        checkpoint = CheckpointState(
            ranges=ranges,
            timestamp=timestamp,
            worker_id=0,
        )

        data = checkpoint.to_dict()

        assert 'ranges' in data
        assert 'timestamp' in data
        assert 'worker_id' in data
        assert data['worker_id'] == 0
        assert isinstance(data['ranges'], list)
        assert isinstance(data['timestamp'], str)


@pytest.mark.unit
class TestCheckpointConfig:
    """Test CheckpointConfig dataclass"""

    def test_default_values(self):
        """Test checkpoint config defaults"""
        config = CheckpointConfig()

        assert config.enabled == False  # Disabled by default
        assert config.storage == 'db'
        assert config.table_prefix == 'amp_'

    def test_custom_values(self):
        """Test checkpoint config with custom values"""
        config = CheckpointConfig(
            enabled=True,
            storage='external',
            table_prefix='custom_',
        )

        assert config.enabled == True
        assert config.storage == 'external'
        assert config.table_prefix == 'custom_'


@pytest.mark.unit
class TestNullCheckpointStore:
    """Test NullCheckpointStore no-op implementation"""

    def test_save_does_nothing(self):
        """Test that save is a no-op"""
        config = CheckpointConfig(enabled=False)
        store = NullCheckpointStore(config)

        checkpoint = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.utcnow(),
        )

        # Should not raise
        store.save('test_conn', 'test_table', checkpoint)

    def test_load_returns_none(self):
        """Test that load always returns None"""
        config = CheckpointConfig(enabled=False)
        store = NullCheckpointStore(config)

        result = store.load('test_conn', 'test_table')

        assert result is None

    def test_delete_for_network_does_nothing(self):
        """Test that delete_for_network is a no-op"""
        config = CheckpointConfig(enabled=False)
        store = NullCheckpointStore(config)

        # Should not raise
        store.delete_for_network('test_conn', 'test_table', 'ethereum')

    def test_delete_does_nothing(self):
        """Test that delete is a no-op"""
        config = CheckpointConfig(enabled=False)
        store = NullCheckpointStore(config)

        # Should not raise
        store.delete('test_conn', 'test_table')
