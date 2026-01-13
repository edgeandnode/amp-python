"""
Unit tests for crash recovery via _rewind_to_watermark() method.

These tests verify the crash recovery logic works correctly in isolation.
"""

from unittest.mock import Mock

import pytest

from src.amp.loaders.base import LoadResult
from src.amp.streaming.types import BlockRange, ResumeWatermark
from tests.fixtures.mock_clients import MockDataLoader


@pytest.fixture
def mock_loader() -> MockDataLoader:
    """Create a mock loader with state store"""
    loader = MockDataLoader({'test': 'config'})
    loader.connect()

    loader.state_store = Mock()
    loader.state_enabled = True

    return loader


@pytest.mark.unit
class TestCrashRecovery:
    """Test _rewind_to_watermark() crash recovery method"""

    def test_rewind_with_no_state(self, mock_loader):
        """Should return early if state_enabled=False"""
        mock_loader.state_enabled = False

        mock_loader._rewind_to_watermark('test_table', 'test_conn')

        mock_loader.state_store.get_resume_position.assert_not_called()

    def test_rewind_with_no_watermark(self, mock_loader):
        """Should return early if no watermark exists"""
        mock_loader.state_store.get_resume_position = Mock(return_value=None)

        mock_loader._rewind_to_watermark('test_table', 'test_conn')

        mock_loader.state_store.get_resume_position.assert_called_once_with('test_conn', 'test_table')

    def test_rewind_calls_handle_reorg(self, mock_loader):
        """Should call _handle_reorg with correct invalidation ranges"""
        watermark = ResumeWatermark(ranges=[BlockRange(network='ethereum', start=1000, end=1010, hash='0xabc')])
        mock_loader.state_store.get_resume_position = Mock(return_value=watermark)
        mock_loader._handle_reorg = Mock()

        mock_loader._rewind_to_watermark('test_table', 'test_conn')

        mock_loader._handle_reorg.assert_called_once()
        call_args = mock_loader._handle_reorg.call_args
        invalidation_ranges = call_args[0][0]
        assert len(invalidation_ranges) == 1
        assert invalidation_ranges[0].network == 'ethereum'
        assert invalidation_ranges[0].start == 1011
        assert call_args[0][1] == 'test_table'
        assert call_args[0][2] == 'test_conn'

    def test_rewind_handles_not_implemented(self, mock_loader):
        """Should gracefully handle loaders without _handle_reorg"""
        watermark = ResumeWatermark(ranges=[BlockRange(network='ethereum', start=1000, end=1010, hash='0xabc')])
        mock_loader.state_store.get_resume_position = Mock(return_value=watermark)
        mock_loader._handle_reorg = Mock(side_effect=NotImplementedError())
        mock_loader.state_store.invalidate_from_block = Mock(return_value=[])

        mock_loader._rewind_to_watermark('test_table', 'test_conn')

        mock_loader.state_store.invalidate_from_block.assert_called_once_with(
            'test_conn', 'test_table', 'ethereum', 1011
        )

    def test_rewind_with_multiple_networks(self, mock_loader):
        """Should process ethereum and polygon separately"""
        watermark = ResumeWatermark(
            ranges=[
                BlockRange(network='ethereum', start=1000, end=1010, hash='0xabc'),
                BlockRange(network='polygon', start=2000, end=2010, hash='0xdef'),
            ]
        )
        mock_loader.state_store.get_resume_position = Mock(return_value=watermark)
        mock_loader._handle_reorg = Mock()

        mock_loader._rewind_to_watermark('test_table', 'test_conn')

        assert mock_loader._handle_reorg.call_count == 2

        first_call = mock_loader._handle_reorg.call_args_list[0]
        assert first_call[0][0][0].network == 'ethereum'
        assert first_call[0][0][0].start == 1011

        second_call = mock_loader._handle_reorg.call_args_list[1]
        assert second_call[0][0][0].network == 'polygon'
        assert second_call[0][0][0].start == 2011

    def test_rewind_with_table_name_none(self, mock_loader):
        """Should return early when table_name=None (not yet implemented)"""
        mock_loader._rewind_to_watermark(table_name=None, connection_name='test_conn')

        mock_loader.state_store.get_resume_position.assert_not_called()

    def test_rewind_uses_default_connection_name(self, mock_loader):
        """Should use default connection name from loader class"""
        watermark = ResumeWatermark(ranges=[BlockRange(network='ethereum', start=1000, end=1010, hash='0xabc')])
        mock_loader.state_store.get_resume_position = Mock(return_value=watermark)
        mock_loader._handle_reorg = Mock()

        mock_loader._rewind_to_watermark('test_table', connection_name=None)

        mock_loader.state_store.get_resume_position.assert_called_once_with('mockdata', 'test_table')
