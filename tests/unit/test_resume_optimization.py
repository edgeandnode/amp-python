"""
Unit tests for resume position optimization in parallel streaming.

Tests the logic that adjusts min_block based on persistent state to skip
already-processed partitions during job resumption.
"""

from unittest.mock import Mock, patch

from amp.streaming.parallel import ParallelConfig, ParallelStreamExecutor
from amp.streaming.types import BlockRange, ResumeWatermark


def test_resume_optimization_adjusts_min_block():
    """Test that min_block is adjusted when resume position is available."""
    # Setup mock client and loader
    mock_client = Mock()
    mock_client.connection_manager.get_connection_info.return_value = {
        'loader': 'snowflake',
        'config': {'state': {'enabled': True, 'storage': 'snowflake'}},
    }

    # Mock loader with state store that has resume position
    mock_loader = Mock()
    mock_state_store = Mock()

    # Resume position: blocks 0-500K already processed (no gaps)
    # When detect_gaps=True, this returns a continuation marker (start == end)
    mock_state_store.get_resume_position.return_value = ResumeWatermark(
        ranges=[
            # Continuation marker: start == end signals "continue from here"
            BlockRange(network='ethereum', start=500_001, end=500_001, hash='0xabc...')
        ]
    )
    mock_loader.state_store = mock_state_store

    # Mock create_loader to return our mock loader
    with patch('amp.loaders.registry.create_loader', return_value=mock_loader):
        # Create parallel config for blocks 0-1M
        original_config = ParallelConfig(
            num_workers=4,
            table_name='eth_firehose.logs',
            min_block=0,
            max_block=1_000_000,
        )

        executor = ParallelStreamExecutor(mock_client, original_config)

        # Call resume optimization
        adjusted_config, resume_watermark, message = executor._get_resume_adjusted_config(
            connection_name='test_conn', destination='test_table', config=original_config
        )

        # Verify min_block was adjusted to 500,001 (max processed + 1)
        assert adjusted_config.min_block == 500_001
        assert adjusted_config.max_block == 1_000_000
        assert message is not None
        assert '500,001' in message or '500,000' in message  # blocks skipped
        assert resume_watermark is None  # No gaps in this scenario


def test_resume_optimization_no_adjustment_when_disabled():
    """Test that no adjustment happens when state management is disabled."""
    mock_client = Mock()
    mock_client.connection_manager.get_connection_info.return_value = {
        'loader': 'snowflake',
        'config': {
            'state': {'enabled': False}  # State disabled
        },
    }

    original_config = ParallelConfig(
        num_workers=4,
        table_name='eth_firehose.logs',
        min_block=0,
        max_block=1_000_000,
    )

    executor = ParallelStreamExecutor(mock_client, original_config)

    adjusted_config, resume_watermark, message = executor._get_resume_adjusted_config(
        connection_name='test_conn', destination='test_table', config=original_config
    )

    # No adjustment when state disabled
    assert adjusted_config.min_block == original_config.min_block
    assert message is None
    assert resume_watermark is None


def test_resume_optimization_no_adjustment_when_no_resume_position():
    """Test that no adjustment happens when no batches have been processed yet."""
    mock_client = Mock()
    mock_client.connection_manager.get_connection_info.return_value = {
        'loader': 'snowflake',
        'config': {'state': {'enabled': True, 'storage': 'snowflake'}},
    }

    mock_loader = Mock()
    mock_state_store = Mock()
    mock_state_store.get_resume_position.return_value = None  # No resume position
    mock_loader.state_store = mock_state_store

    with patch('amp.loaders.registry.create_loader', return_value=mock_loader):
        original_config = ParallelConfig(
            num_workers=4,
            table_name='eth_firehose.logs',
            min_block=0,
            max_block=1_000_000,
        )

        executor = ParallelStreamExecutor(mock_client, original_config)

        adjusted_config, resume_watermark, message = executor._get_resume_adjusted_config(
            connection_name='test_conn', destination='test_table', config=original_config
        )

        # No adjustment when no resume position
        assert adjusted_config.min_block == original_config.min_block
        assert message is None
        assert resume_watermark is None


def test_resume_optimization_no_adjustment_when_resume_behind_min():
    """Test that no adjustment happens when resume position is behind min_block."""
    mock_client = Mock()
    mock_client.connection_manager.get_connection_info.return_value = {
        'loader': 'snowflake',
        'config': {'state': {'enabled': True, 'storage': 'snowflake'}},
    }

    mock_loader = Mock()
    mock_state_store = Mock()

    # Resume position at 100K, but we're starting from 500K (no gaps in our range)
    # When detect_gaps=True, this returns continuation marker at 100,001
    mock_state_store.get_resume_position.return_value = ResumeWatermark(
        ranges=[
            # Continuation marker at 100,001 (but we're starting at 500K so this should be ignored)
            BlockRange(network='ethereum', start=100_001, end=100_001, hash='0xdef...')
        ]
    )
    mock_loader.state_store = mock_state_store

    with patch('amp.loaders.registry.create_loader', return_value=mock_loader):
        original_config = ParallelConfig(
            num_workers=4,
            table_name='eth_firehose.logs',
            min_block=500_000,  # Starting from 500K
            max_block=1_000_000,
        )

        executor = ParallelStreamExecutor(mock_client, original_config)

        adjusted_config, resume_watermark, message = executor._get_resume_adjusted_config(
            connection_name='test_conn', destination='test_table', config=original_config
        )

        # No adjustment when resume position is behind min_block
        assert adjusted_config.min_block == original_config.min_block
        assert message is None
        assert resume_watermark is None
