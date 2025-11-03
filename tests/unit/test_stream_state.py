"""
Unit tests for unified stream state management system.

Tests the new StreamState architecture that replaces separate checkpoint
and processedRanges systems with a single unified mechanism.
"""

import pytest
from datetime import datetime

from amp.streaming.state import (
    BatchIdentifier,
    InMemoryStreamStateStore,
    NullStreamStateStore,
    ProcessedBatch,
)
from amp.streaming.types import BlockRange, ResumeWatermark


class TestBatchIdentifier:
    """Test BatchIdentifier creation and properties."""

    def test_create_from_block_range(self):
        """Test creating BatchIdentifier from BlockRange with hash."""
        block_range = BlockRange(
            network="ethereum",
            start=100,
            end=200,
            hash="0xabc123",
            prev_hash="0xdef456"
        )

        batch_id = BatchIdentifier.from_block_range(block_range)

        assert batch_id.network == "ethereum"
        assert batch_id.start_block == 100
        assert batch_id.end_block == 200
        assert batch_id.end_hash == "0xabc123"
        assert batch_id.start_parent_hash == "0xdef456"

    def test_create_from_block_range_no_hash_generates_synthetic(self):
        """Test that creating BatchIdentifier without hash generates synthetic hash."""
        block_range = BlockRange(
            network="ethereum",
            start=100,
            end=200
        )

        batch_id = BatchIdentifier.from_block_range(block_range)

        # Should generate synthetic hash from position
        assert batch_id.network == "ethereum"
        assert batch_id.start_block == 100
        assert batch_id.end_block == 200
        assert batch_id.end_hash is not None
        assert len(batch_id.end_hash) == 64  # SHA256 hex digest
        assert batch_id.start_parent_hash == ""  # No prev_hash provided

    def test_unique_id_is_deterministic(self):
        """Test that same input produces same unique_id."""
        batch_id1 = BatchIdentifier(
            network="ethereum",
            start_block=100,
            end_block=200,
            end_hash="0xabc123",
            start_parent_hash="0xdef456"
        )

        batch_id2 = BatchIdentifier(
            network="ethereum",
            start_block=100,
            end_block=200,
            end_hash="0xabc123",
            start_parent_hash="0xdef456"
        )

        assert batch_id1.unique_id == batch_id2.unique_id
        assert len(batch_id1.unique_id) == 16  # 16 hex chars

    def test_unique_id_differs_with_different_hash(self):
        """Test that different block hashes produce different unique_ids."""
        batch_id1 = BatchIdentifier(
            network="ethereum",
            start_block=100,
            end_block=200,
            end_hash="0xabc123",
            start_parent_hash="0xdef456"
        )

        batch_id2 = BatchIdentifier(
            network="ethereum",
            start_block=100,
            end_block=200,
            end_hash="0xdifferent",  # Different hash
            start_parent_hash="0xdef456"
        )

        assert batch_id1.unique_id != batch_id2.unique_id

    def test_position_key(self):
        """Test position_key property."""
        batch_id = BatchIdentifier(
            network="polygon",
            start_block=500,
            end_block=600,
            end_hash="0xabc",
        )

        assert batch_id.position_key == ("polygon", 500, 600)

    def test_to_block_range(self):
        """Test converting BatchIdentifier back to BlockRange."""
        batch_id = BatchIdentifier(
            network="arbitrum",
            start_block=1000,
            end_block=2000,
            end_hash="0x123",
            start_parent_hash="0x456"
        )

        block_range = batch_id.to_block_range()

        assert block_range.network == "arbitrum"
        assert block_range.start == 1000
        assert block_range.end == 2000
        assert block_range.hash == "0x123"
        assert block_range.prev_hash == "0x456"

    def test_overlaps_or_after(self):
        """Test overlap detection for reorg invalidation."""
        batch_id = BatchIdentifier(
            network="ethereum",
            start_block=100,
            end_block=200,
            end_hash="0xabc"
        )

        # Batch ends at 200, so it overlaps with reorg at 150
        assert batch_id.overlaps_or_after(150) is True

        # Also overlaps at end block
        assert batch_id.overlaps_or_after(200) is True

        # Doesn't overlap with reorg after end
        assert batch_id.overlaps_or_after(201) is False

        # Overlaps with reorg before start (end >= from_block)
        assert batch_id.overlaps_or_after(50) is True

    def test_batch_identifier_is_hashable(self):
        """Test that BatchIdentifier can be used in sets."""
        batch_id1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch_id2 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch_id3 = BatchIdentifier("ethereum", 100, 200, "0xdef")

        # Same values should be equal
        assert batch_id1 == batch_id2

        # Can be added to sets
        batch_set = {batch_id1, batch_id2, batch_id3}
        assert len(batch_set) == 2  # batch_id1 and batch_id2 are duplicate


class TestInMemoryStreamStateStore:
    """Test in-memory stream state store."""

    def test_mark_and_check_processed(self):
        """Test marking batches as processed and checking."""
        store = InMemoryStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")

        # Initially not processed
        assert store.is_processed("conn1", "table1", [batch_id]) is False

        # Mark as processed
        store.mark_processed("conn1", "table1", [batch_id])

        # Now should be processed
        assert store.is_processed("conn1", "table1", [batch_id]) is True

    def test_multiple_batches_all_must_be_processed(self):
        """Test that all batches must be processed for is_processed to return True."""
        store = InMemoryStreamStateStore()

        batch_id1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch_id2 = BatchIdentifier("ethereum", 200, 300, "0xdef")

        # Mark only first batch
        store.mark_processed("conn1", "table1", [batch_id1])

        # Checking both should return False (second not processed)
        assert store.is_processed("conn1", "table1", [batch_id1, batch_id2]) is False

        # Mark second batch
        store.mark_processed("conn1", "table1", [batch_id2])

        # Now both are processed
        assert store.is_processed("conn1", "table1", [batch_id1, batch_id2]) is True

    def test_separate_networks(self):
        """Test that different networks are tracked separately."""
        store = InMemoryStreamStateStore()

        eth_batch = BatchIdentifier("ethereum", 100, 200, "0xabc")
        poly_batch = BatchIdentifier("polygon", 100, 200, "0xdef")

        store.mark_processed("conn1", "table1", [eth_batch])

        assert store.is_processed("conn1", "table1", [eth_batch]) is True
        assert store.is_processed("conn1", "table1", [poly_batch]) is False

    def test_separate_connections_and_tables(self):
        """Test that different connections and tables are isolated."""
        store = InMemoryStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")

        store.mark_processed("conn1", "table1", [batch_id])

        # Same batch, different connection
        assert store.is_processed("conn2", "table1", [batch_id]) is False

        # Same batch, different table
        assert store.is_processed("conn1", "table2", [batch_id]) is False

    def test_get_resume_position_empty(self):
        """Test getting resume position when no batches processed."""
        store = InMemoryStreamStateStore()

        watermark = store.get_resume_position("conn1", "table1")

        assert watermark is None

    def test_get_resume_position_single_network(self):
        """Test getting resume position for single network."""
        store = InMemoryStreamStateStore()

        # Process batches in order
        batch1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch2 = BatchIdentifier("ethereum", 200, 300, "0xdef")
        batch3 = BatchIdentifier("ethereum", 300, 400, "0x123")

        store.mark_processed("conn1", "table1", [batch1])
        store.mark_processed("conn1", "table1", [batch2])
        store.mark_processed("conn1", "table1", [batch3])

        watermark = store.get_resume_position("conn1", "table1")

        assert watermark is not None
        assert len(watermark.ranges) == 1
        assert watermark.ranges[0].network == "ethereum"
        assert watermark.ranges[0].end == 400  # Max block

    def test_get_resume_position_multiple_networks(self):
        """Test getting resume position for multiple networks."""
        store = InMemoryStreamStateStore()

        eth_batch = BatchIdentifier("ethereum", 100, 200, "0xabc")
        poly_batch = BatchIdentifier("polygon", 500, 600, "0xdef")
        arb_batch = BatchIdentifier("arbitrum", 1000, 1100, "0x123")

        store.mark_processed("conn1", "table1", [eth_batch])
        store.mark_processed("conn1", "table1", [poly_batch])
        store.mark_processed("conn1", "table1", [arb_batch])

        watermark = store.get_resume_position("conn1", "table1")

        assert watermark is not None
        assert len(watermark.ranges) == 3

        # Check each network has correct max block
        networks = {r.network: r.end for r in watermark.ranges}
        assert networks["ethereum"] == 200
        assert networks["polygon"] == 600
        assert networks["arbitrum"] == 1100

    def test_invalidate_from_block(self):
        """Test invalidating batches from a specific block (reorg)."""
        store = InMemoryStreamStateStore()

        # Process several batches
        batch1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch2 = BatchIdentifier("ethereum", 200, 300, "0xdef")
        batch3 = BatchIdentifier("ethereum", 300, 400, "0x123")

        store.mark_processed("conn1", "table1", [batch1, batch2, batch3])

        # Invalidate from block 250 (should remove batch2 and batch3)
        invalidated = store.invalidate_from_block("conn1", "table1", "ethereum", 250)

        # batch2 ends at 300 (>= 250), batch3 ends at 400 (>= 250)
        assert len(invalidated) == 2
        assert batch2 in invalidated
        assert batch3 in invalidated

        # batch1 should still be processed
        assert store.is_processed("conn1", "table1", [batch1]) is True

        # batch2 and batch3 should no longer be processed
        assert store.is_processed("conn1", "table1", [batch2]) is False
        assert store.is_processed("conn1", "table1", [batch3]) is False

    def test_invalidate_only_affects_specified_network(self):
        """Test that reorg invalidation only affects the specified network."""
        store = InMemoryStreamStateStore()

        eth_batch = BatchIdentifier("ethereum", 100, 200, "0xabc")
        poly_batch = BatchIdentifier("polygon", 100, 200, "0xdef")

        store.mark_processed("conn1", "table1", [eth_batch, poly_batch])

        # Invalidate ethereum from block 150
        invalidated = store.invalidate_from_block("conn1", "table1", "ethereum", 150)

        assert len(invalidated) == 1
        assert eth_batch in invalidated

        # Polygon batch should still be processed
        assert store.is_processed("conn1", "table1", [poly_batch]) is True

    def test_cleanup_before_block(self):
        """Test cleaning up old batches before a given block."""
        store = InMemoryStreamStateStore()

        # Process batches
        batch1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch2 = BatchIdentifier("ethereum", 200, 300, "0xdef")
        batch3 = BatchIdentifier("ethereum", 300, 400, "0x123")

        store.mark_processed("conn1", "table1", [batch1, batch2, batch3])

        # Cleanup batches before block 250
        # This should remove batch1 (ends at 200 < 250)
        store.cleanup_before_block("conn1", "table1", "ethereum", 250)

        # batch1 should be removed
        assert store.is_processed("conn1", "table1", [batch1]) is False

        # batch2 and batch3 should still be there (end >= 250)
        assert store.is_processed("conn1", "table1", [batch2]) is True
        assert store.is_processed("conn1", "table1", [batch3]) is True


class TestNullStreamStateStore:
    """Test null stream state store (no-op implementation)."""

    def test_is_processed_always_false(self):
        """Test that null store always returns False for is_processed."""
        store = NullStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")

        assert store.is_processed("conn1", "table1", [batch_id]) is False

    def test_mark_processed_is_noop(self):
        """Test that marking as processed does nothing."""
        store = NullStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")

        store.mark_processed("conn1", "table1", [batch_id])

        # Still returns False
        assert store.is_processed("conn1", "table1", [batch_id]) is False

    def test_get_resume_position_always_none(self):
        """Test that null store always returns None for resume position."""
        store = NullStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")
        store.mark_processed("conn1", "table1", [batch_id])

        assert store.get_resume_position("conn1", "table1") is None

    def test_invalidate_returns_empty_list(self):
        """Test that invalidation returns empty list."""
        store = NullStreamStateStore()

        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc")
        store.mark_processed("conn1", "table1", [batch_id])

        invalidated = store.invalidate_from_block("conn1", "table1", "ethereum", 150)

        assert invalidated == []


class TestProcessedBatch:
    """Test ProcessedBatch data class."""

    def test_create_and_serialize(self):
        """Test creating and serializing ProcessedBatch."""
        batch_id = BatchIdentifier("ethereum", 100, 200, "0xabc", "0xdef")
        processed_batch = ProcessedBatch(batch_id=batch_id)

        data = processed_batch.to_dict()

        assert data["network"] == "ethereum"
        assert data["start_block"] == 100
        assert data["end_block"] == 200
        assert data["end_hash"] == "0xabc"
        assert data["start_parent_hash"] == "0xdef"
        assert data["unique_id"] == batch_id.unique_id
        assert "processed_at" in data
        assert data["reorg_invalidation"] is False

    def test_deserialize(self):
        """Test deserializing ProcessedBatch from dict."""
        data = {
            "network": "polygon",
            "start_block": 500,
            "end_block": 600,
            "end_hash": "0x123",
            "start_parent_hash": "0x456",
            "unique_id": "abc123",
            "processed_at": "2024-01-01T00:00:00",
            "reorg_invalidation": False
        }

        processed_batch = ProcessedBatch.from_dict(data)

        assert processed_batch.batch_id.network == "polygon"
        assert processed_batch.batch_id.start_block == 500
        assert processed_batch.batch_id.end_block == 600
        assert processed_batch.batch_id.end_hash == "0x123"
        assert processed_batch.reorg_invalidation is False


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_streaming_with_resume(self):
        """Test streaming session with resume after interruption."""
        store = InMemoryStreamStateStore()

        # Session 1: Process some batches
        batch1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch2 = BatchIdentifier("ethereum", 200, 300, "0xdef")

        store.mark_processed("conn1", "transfers", [batch1])
        store.mark_processed("conn1", "transfers", [batch2])

        # Get resume position
        watermark = store.get_resume_position("conn1", "transfers")
        assert watermark.ranges[0].end == 300

        # Session 2: Resume from watermark, process more batches
        batch3 = BatchIdentifier("ethereum", 300, 400, "0x123")
        batch4 = BatchIdentifier("ethereum", 400, 500, "0x456")

        # Check that previous batches are already processed (idempotency)
        assert store.is_processed("conn1", "transfers", [batch2]) is True

        # Process new batches
        store.mark_processed("conn1", "transfers", [batch3])
        store.mark_processed("conn1", "transfers", [batch4])

        # New resume position
        watermark = store.get_resume_position("conn1", "transfers")
        assert watermark.ranges[0].end == 500

    def test_reorg_scenario(self):
        """Test blockchain reorganization scenario."""
        store = InMemoryStreamStateStore()

        # Process batches
        batch1 = BatchIdentifier("ethereum", 100, 200, "0xabc")
        batch2 = BatchIdentifier("ethereum", 200, 300, "0xdef")
        batch3 = BatchIdentifier("ethereum", 300, 400, "0x123")

        store.mark_processed("conn1", "blocks", [batch1, batch2, batch3])

        # Reorg detected at block 250
        # Invalidate all batches from block 250 onwards
        invalidated = store.invalidate_from_block("conn1", "blocks", "ethereum", 250)

        # batch2 (200-300) and batch3 (300-400) should be invalidated
        assert len(invalidated) == 2

        # Resume position should now be batch1's end
        watermark = store.get_resume_position("conn1", "blocks")
        assert watermark.ranges[0].end == 200

        # Re-process from block 250 with new chain data (different hashes)
        batch2_new = BatchIdentifier("ethereum", 200, 300, "0xNEWHASH1")
        batch3_new = BatchIdentifier("ethereum", 300, 400, "0xNEWHASH2")

        store.mark_processed("conn1", "blocks", [batch2_new, batch3_new])

        # Both old and new versions should be tracked separately
        assert store.is_processed("conn1", "blocks", [batch2_new]) is True
        assert store.is_processed("conn1", "blocks", [batch2]) is False  # Old version was invalidated

    def test_multi_network_streaming(self):
        """Test streaming from multiple networks simultaneously."""
        store = InMemoryStreamStateStore()

        # Process batches from different networks
        eth_batch1 = BatchIdentifier("ethereum", 100, 200, "0xeth1")
        eth_batch2 = BatchIdentifier("ethereum", 200, 300, "0xeth2")
        poly_batch1 = BatchIdentifier("polygon", 500, 600, "0xpoly1")
        arb_batch1 = BatchIdentifier("arbitrum", 1000, 1100, "0xarb1")

        store.mark_processed("conn1", "transfers", [eth_batch1, eth_batch2])
        store.mark_processed("conn1", "transfers", [poly_batch1])
        store.mark_processed("conn1", "transfers", [arb_batch1])

        # Get resume position for all networks
        watermark = store.get_resume_position("conn1", "transfers")

        assert len(watermark.ranges) == 3
        networks = {r.network: r.end for r in watermark.ranges}
        assert networks["ethereum"] == 300
        assert networks["polygon"] == 600
        assert networks["arbitrum"] == 1100

        # Reorg on ethereum only
        invalidated = store.invalidate_from_block("conn1", "transfers", "ethereum", 250)
        assert len(invalidated) == 1  # Only eth_batch2

        # Other networks unaffected
        assert store.is_processed("conn1", "transfers", [poly_batch1]) is True
        assert store.is_processed("conn1", "transfers", [arb_batch1]) is True
