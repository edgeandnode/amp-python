"""
Unit tests for streaming helper methods in DataLoader.

These tests verify the individual helper methods extracted from load_stream_continuous,
ensuring each piece of logic works correctly in isolation.
"""

import time
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

import pytest
import pyarrow as pa

from src.amp.loaders.base import DataLoader, LoadResult
from src.amp.streaming.types import BlockRange, ResponseBatch, ResponseBatchWithReorg, BatchMetadata
from src.amp.streaming.checkpoint import CheckpointState
from tests.fixtures.mock_clients import MockDataLoader


@pytest.fixture
def mock_loader():
    """Create a mock loader with all resilience components mocked"""
    loader = MockDataLoader({'test': 'config'})
    loader.connect()

    # Mock resilience components
    loader.checkpoint_store = Mock()
    loader.processed_ranges_store = Mock()
    loader.idempotency_config = Mock(enabled=True, verification_hash=False)

    return loader


@pytest.fixture
def sample_batch():
    """Create a sample PyArrow batch for testing"""
    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
    ])
    data = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(['a', 'b', 'c'])],
        schema=schema
    )
    return data


@pytest.fixture
def sample_ranges():
    """Create sample block ranges for testing"""
    return [
        BlockRange(network='ethereum', start=100, end=200),
        BlockRange(network='polygon', start=300, end=400),
    ]


@pytest.mark.unit
class TestProcessReorgEvent:
    """Test _process_reorg_event helper method"""

    def test_successful_reorg_processing(self, mock_loader, sample_ranges):
        """Test successful reorg event processing"""
        # Setup
        mock_loader._handle_reorg = Mock()
        mock_loader._compute_reorg_resume_point = Mock(return_value=sample_ranges)
        mock_loader.checkpoint_store.save = Mock()

        response = Mock()
        response.invalidation_ranges = sample_ranges

        # Execute
        start_time = time.time()
        result = mock_loader._process_reorg_event(
            response=response,
            table_name='test_table',
            connection_name='test_conn',
            worker_id=0,
            reorg_count=3,
            start_time=start_time
        )

        # Verify
        assert result.success
        assert result.is_reorg
        assert result.rows_loaded == 0
        assert result.table_name == 'test_table'
        assert result.invalidation_ranges == sample_ranges
        assert result.metadata['operation'] == 'reorg'
        assert result.metadata['invalidation_count'] == 2
        assert result.metadata['reorg_number'] == 3

        # Verify method calls
        mock_loader._handle_reorg.assert_called_once_with(sample_ranges, 'test_table')
        # Verify reorg checkpoint was saved
        mock_loader.checkpoint_store.save.assert_called_once()
        call_args = mock_loader.checkpoint_store.save.call_args
        assert call_args[0][0] == 'test_conn'
        assert call_args[0][1] == 'test_table'
        checkpoint = call_args[0][2]
        assert checkpoint.is_reorg is True

    def test_reorg_with_no_invalidation_ranges(self, mock_loader):
        """Test reorg event with no invalidation ranges"""
        # Setup
        mock_loader._handle_reorg = Mock()
        mock_loader.checkpoint_store.save = Mock()

        response = Mock()
        response.invalidation_ranges = []

        # Execute
        start_time = time.time()
        result = mock_loader._process_reorg_event(
            response=response,
            table_name='test_table',
            connection_name='test_conn',
            reorg_count=1,
            start_time=start_time
        )

        # Verify
        assert result.success
        assert result.is_reorg
        assert result.metadata['invalidation_count'] == 0

    def test_reorg_handler_failure(self, mock_loader, sample_ranges):
        """Test error handling when reorg handler fails"""
        # Setup
        mock_loader._handle_reorg = Mock(side_effect=Exception('Reorg failed'))

        response = Mock()
        response.invalidation_ranges = sample_ranges

        # Execute and verify exception is raised
        with pytest.raises(Exception, match='Reorg failed'):
            mock_loader._process_reorg_event(
                response=response,
                table_name='test_table',
                connection_name='test_conn',
                reorg_count=1,
                start_time=time.time()
            )


@pytest.mark.unit
class TestProcessBatchTransactional:
    """Test _process_batch_transactional helper method"""

    def test_successful_transactional_load(self, mock_loader, sample_batch, sample_ranges):
        """Test successful transactional batch load"""
        # Setup
        mock_loader.load_batch_transactional = Mock(return_value=3)  # 3 rows loaded

        # Execute
        result = mock_loader._process_batch_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify
        assert result.success
        assert result.rows_loaded == 3
        assert result.table_name == 'test_table'
        assert result.metadata['operation'] == 'transactional_load'
        assert result.metadata['ranges'] == [r.to_dict() for r in sample_ranges]
        assert result.ops_per_second > 0

        # Verify method call
        mock_loader.load_batch_transactional.assert_called_once_with(
            sample_batch, 'test_table', 'test_conn', sample_ranges, 'hash123'
        )

    def test_transactional_duplicate_detection(self, mock_loader, sample_batch, sample_ranges):
        """Test transactional batch with duplicate detection (0 rows)"""
        # Setup - 0 rows means duplicate was detected
        mock_loader.load_batch_transactional = Mock(return_value=0)

        # Execute
        result = mock_loader._process_batch_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify
        assert result.success
        assert result.rows_loaded == 0
        assert result.metadata['operation'] == 'skip_duplicate'

    def test_transactional_load_failure(self, mock_loader, sample_batch, sample_ranges):
        """Test transactional load failure and error handling"""
        # Setup
        mock_loader.load_batch_transactional = Mock(
            side_effect=Exception('Transaction failed')
        )

        # Execute
        result = mock_loader._process_batch_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify error result
        assert not result.success
        assert result.rows_loaded == 0
        assert 'Transaction failed' in result.error
        assert result.ops_per_second == 0


@pytest.mark.unit
class TestProcessBatchNonTransactional:
    """Test _process_batch_non_transactional helper method"""

    def test_successful_non_transactional_load(self, mock_loader, sample_batch, sample_ranges):
        """Test successful non-transactional batch load"""
        # Setup
        mock_loader.processed_ranges_store.is_processed = Mock(return_value=False)
        mock_loader.processed_ranges_store.mark_processed = Mock()

        # Mock load_batch to return success
        success_result = LoadResult(
            rows_loaded=3,
            duration=0.1,
            ops_per_second=30.0,
            table_name='test_table',
            loader_type='mock',
            success=True
        )
        mock_loader.load_batch = Mock(return_value=success_result)

        # Execute
        result = mock_loader._process_batch_non_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify
        assert result.success
        assert result.rows_loaded == 3

        # Verify method calls
        mock_loader.processed_ranges_store.is_processed.assert_called_once_with(
            'test_conn', 'test_table', sample_ranges
        )
        mock_loader.load_batch.assert_called_once()
        mock_loader.processed_ranges_store.mark_processed.assert_called_once_with(
            'test_conn', 'test_table', sample_ranges, 'hash123'
        )

    def test_duplicate_detection_returns_skip_result(self, mock_loader, sample_batch, sample_ranges):
        """Test duplicate detection returns skip result"""
        # Setup - is_processed returns True
        mock_loader.processed_ranges_store.is_processed = Mock(return_value=True)
        mock_loader.load_batch = Mock()  # Should not be called

        # Execute
        result = mock_loader._process_batch_non_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify
        assert result.success
        assert result.rows_loaded == 0
        assert result.metadata['operation'] == 'skip_duplicate'
        assert result.metadata['ranges'] == [r.to_dict() for r in sample_ranges]

        # load_batch should not be called for duplicates
        mock_loader.load_batch.assert_not_called()

    def test_no_ranges_skips_duplicate_check(self, mock_loader, sample_batch):
        """Test that no ranges means no duplicate checking"""
        # Setup
        mock_loader.processed_ranges_store.is_processed = Mock()
        success_result = LoadResult(
            rows_loaded=3,
            duration=0.1,
            ops_per_second=30.0,
            table_name='test_table',
            loader_type='mock',
            success=True
        )
        mock_loader.load_batch = Mock(return_value=success_result)

        # Execute with None ranges
        result = mock_loader._process_batch_non_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=None,
            batch_hash=None
        )

        # Verify
        assert result.success

        # is_processed should not be called
        mock_loader.processed_ranges_store.is_processed.assert_not_called()

    def test_mark_processed_failure_continues(self, mock_loader, sample_batch, sample_ranges):
        """Test that mark_processed failure doesn't fail the load"""
        # Setup
        mock_loader.processed_ranges_store.is_processed = Mock(return_value=False)
        mock_loader.processed_ranges_store.mark_processed = Mock(
            side_effect=Exception('Mark failed')
        )

        success_result = LoadResult(
            rows_loaded=3,
            duration=0.1,
            ops_per_second=30.0,
            table_name='test_table',
            loader_type='mock',
            success=True
        )
        mock_loader.load_batch = Mock(return_value=success_result)

        # Execute - should not raise exception
        result = mock_loader._process_batch_non_transactional(
            batch_data=sample_batch,
            table_name='test_table',
            connection_name='test_conn',
            ranges=sample_ranges,
            batch_hash='hash123'
        )

        # Verify - load still succeeded despite mark_processed failure
        assert result.success
        assert result.rows_loaded == 3


@pytest.mark.unit
class TestSaveCheckpointIfComplete:
    """Test _save_checkpoint_if_complete helper method"""

    def test_saves_checkpoint_when_complete(self, mock_loader, sample_ranges):
        """Test checkpoint is saved when ranges_complete is True"""
        # Setup
        mock_loader.checkpoint_store.save = Mock()

        # Execute
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2024, 1, 1, 12, 0, 0)

            mock_loader._save_checkpoint_if_complete(
                ranges=sample_ranges,
                ranges_complete=True,
                connection_name='test_conn',
                table_name='test_table',
                worker_id=0,
                batch_count=10
            )

        # Verify checkpoint was saved
        mock_loader.checkpoint_store.save.assert_called_once()
        call_args = mock_loader.checkpoint_store.save.call_args

        assert call_args[0][0] == 'test_conn'
        assert call_args[0][1] == 'test_table'

        checkpoint = call_args[0][2]
        assert isinstance(checkpoint, CheckpointState)
        assert checkpoint.ranges == sample_ranges
        assert checkpoint.worker_id == 0

    def test_no_save_when_not_complete(self, mock_loader, sample_ranges):
        """Test checkpoint is not saved when ranges_complete is False"""
        # Setup
        mock_loader.checkpoint_store.save = Mock()

        # Execute
        mock_loader._save_checkpoint_if_complete(
            ranges=sample_ranges,
            ranges_complete=False,  # Not complete
            connection_name='test_conn',
            table_name='test_table',
            worker_id=0,
            batch_count=10
        )

        # Verify no checkpoint was saved
        mock_loader.checkpoint_store.save.assert_not_called()

    def test_no_save_when_no_ranges(self, mock_loader):
        """Test checkpoint is not saved when ranges is empty"""
        # Setup
        mock_loader.checkpoint_store.save = Mock()

        # Execute
        mock_loader._save_checkpoint_if_complete(
            ranges=[],  # Empty ranges
            ranges_complete=True,
            connection_name='test_conn',
            table_name='test_table',
            worker_id=0,
            batch_count=10
        )

        # Verify no checkpoint was saved
        mock_loader.checkpoint_store.save.assert_not_called()

    def test_checkpoint_save_failure_doesnt_raise(self, mock_loader, sample_ranges):
        """Test that checkpoint save failure is logged but doesn't raise"""
        # Setup
        mock_loader.checkpoint_store.save = Mock(side_effect=Exception('Save failed'))

        # Execute - should not raise exception
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2024, 1, 1, 12, 0, 0)

            mock_loader._save_checkpoint_if_complete(
                ranges=sample_ranges,
                ranges_complete=True,
                connection_name='test_conn',
                table_name='test_table',
                worker_id=0,
                batch_count=10
            )

        # Verify save was attempted
        mock_loader.checkpoint_store.save.assert_called_once()


@pytest.mark.unit
class TestAugmentStreamingResult:
    """Test _augment_streaming_result helper method"""

    def test_augments_result_with_ranges(self, mock_loader, sample_ranges):
        """Test result is augmented with streaming metadata including ranges"""
        # Setup
        result = LoadResult(
            rows_loaded=10,
            duration=1.0,
            ops_per_second=10.0,
            table_name='test_table',
            loader_type='mock',
            success=True
        )

        # Execute
        augmented = mock_loader._augment_streaming_result(
            result=result,
            batch_count=5,
            ranges=sample_ranges,
            ranges_complete=True
        )

        # Verify
        assert augmented.metadata['is_streaming'] is True
        assert augmented.metadata['batch_count'] == 5
        assert augmented.metadata['ranges_complete'] is True
        assert 'block_ranges' in augmented.metadata
        assert len(augmented.metadata['block_ranges']) == 2

        # Check block range format
        block_range = augmented.metadata['block_ranges'][0]
        assert 'network' in block_range
        assert 'start' in block_range
        assert 'end' in block_range

    def test_augments_result_without_ranges(self, mock_loader):
        """Test result is augmented without block ranges when ranges is None"""
        # Setup
        result = LoadResult(
            rows_loaded=10,
            duration=1.0,
            ops_per_second=10.0,
            table_name='test_table',
            loader_type='mock',
            success=True
        )

        # Execute
        augmented = mock_loader._augment_streaming_result(
            result=result,
            batch_count=5,
            ranges=None,
            ranges_complete=False
        )

        # Verify
        assert augmented.metadata['is_streaming'] is True
        assert augmented.metadata['batch_count'] == 5
        assert augmented.metadata['ranges_complete'] is False
        assert 'block_ranges' not in augmented.metadata

    def test_preserves_existing_metadata(self, mock_loader, sample_ranges):
        """Test that existing metadata is preserved"""
        # Setup
        result = LoadResult(
            rows_loaded=10,
            duration=1.0,
            ops_per_second=10.0,
            table_name='test_table',
            loader_type='mock',
            success=True,
            metadata={'custom_key': 'custom_value'}
        )

        # Execute
        augmented = mock_loader._augment_streaming_result(
            result=result,
            batch_count=5,
            ranges=sample_ranges,
            ranges_complete=True
        )

        # Verify existing metadata is preserved
        assert augmented.metadata['custom_key'] == 'custom_value'
        assert augmented.metadata['is_streaming'] is True
