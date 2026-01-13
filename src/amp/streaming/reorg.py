"""
Reorg-aware streaming wrapper that detects blockchain reorganizations.
"""

import logging
from typing import Dict, Iterator, List

from .iterator import StreamingResultIterator
from .types import BlockRange, ResponseBatch, ResumeWatermark


class ReorgAwareStream:
    """
    Wraps a streaming result iterator to detect and signal blockchain reorganizations.

    This class monitors the block ranges in consecutive batches to detect chain
    reorganizations (reorgs). When a reorg is detected, a ResponseBatch with
    is_reorg=True is emitted containing the invalidation ranges.

    Supports cross-restart reorg detection by initializing from a resume watermark
    that contains the last known block hashes from persistent state.
    """

    def __init__(self, stream_iterator: StreamingResultIterator, resume_watermark: ResumeWatermark = None):
        """
        Initialize the reorg-aware stream.

        Args:
            stream_iterator: The underlying streaming result iterator
            resume_watermark: Optional watermark from persistent state (LMDB) containing
                              last known block ranges with hashes for cross-restart reorg detection
        """
        self.stream_iterator = stream_iterator
        self.prev_ranges_by_network: Dict[str, BlockRange] = {}
        self.logger = logging.getLogger(__name__)

        if resume_watermark:
            for block_range in resume_watermark.ranges:
                self.prev_ranges_by_network[block_range.network] = block_range
                self.logger.debug(
                    f'Initialized reorg detection for {block_range.network} '
                    f'from block {block_range.end} hash {block_range.hash}'
                )

    def __iter__(self) -> Iterator[ResponseBatch]:
        """Return iterator instance"""
        return self

    def __next__(self) -> ResponseBatch:
        """
        Get the next item from the stream, detecting reorgs.

        Returns:
            ResponseBatch with is_reorg flag set if reorg detected

        Raises:
            StopIteration: When stream is exhausted
            KeyboardInterrupt: When user cancels the stream
        """
        try:
            # Get next batch from underlying stream
            batch = next(self.stream_iterator)

            # Note: ranges_complete flag is handled by CheckpointStore in load_stream_continuous
            # Check if this batch contains only duplicate ranges
            if self._is_duplicate_batch(batch.metadata.ranges):
                self.logger.debug(f'Skipping duplicate batch with ranges: {batch.metadata.ranges}')
                # Recursively call to get the next non-duplicate batch
                return self.__next__()

            # Detect reorgs by comparing with previous ranges
            invalidation_ranges = self._detect_reorg(batch.metadata.ranges)

            # Update previous ranges for each network
            for range in batch.metadata.ranges:
                self.prev_ranges_by_network[range.network] = range

            # If we detected a reorg, yield the reorg notification first
            if invalidation_ranges:
                self.logger.info(f'Reorg detected with {len(invalidation_ranges)} invalidation ranges')
                # Store the batch to yield after the reorg
                self._pending_batch = batch
                return ResponseBatch.reorg_batch(invalidation_ranges)

            # Check if we have a pending batch from a previous reorg detection
            # REVIEW: I think we should remove this
            if hasattr(self, '_pending_batch'):
                pending = self._pending_batch
                delattr(self, '_pending_batch')
                return pending

            # Normal case - just return the data batch
            return batch

        except KeyboardInterrupt:
            self.logger.info('Reorg-aware stream cancelled by user')
            self.stream_iterator.close()
            raise

    def _detect_reorg(self, current_ranges: List[BlockRange]) -> List[BlockRange]:
        """
        Detect reorganizations by comparing current ranges with previous ranges.

        A reorg is detected when either:
        1. Block number overlap: current range starts at or before previous range end
        2. Hash mismatch: server's prev_hash doesn't match our stored hash (cross-restart detection)

        Args:
            current_ranges: Block ranges from the current batch

        Returns:
            List of block ranges that should be invalidated due to reorg
        """
        invalidation_ranges = []

        for current_range in current_ranges:
            prev_range = self.prev_ranges_by_network.get(current_range.network)

            if prev_range:
                is_reorg = False

                # Detection 1: Block number overlap (original logic)
                if current_range != prev_range and current_range.start <= prev_range.end:
                    is_reorg = True
                    self.logger.info(
                        f'Reorg detected via block overlap: {current_range.network} '
                        f'current start {current_range.start} <= prev end {prev_range.end}'
                    )

                # Detection 2: Hash mismatch (cross-restart detection)
                # Server sends prev_hash = hash of block before current range
                # If it doesn't match our stored hash, chain has changed
                elif (
                    current_range.prev_hash is not None
                    and prev_range.hash is not None
                    and current_range.prev_hash != prev_range.hash
                ):
                    is_reorg = True
                    self.logger.info(
                        f'Reorg detected via hash mismatch: {current_range.network} '
                        f'server prev_hash {current_range.prev_hash} != stored hash {prev_range.hash}'
                    )

                if is_reorg:
                    invalidation = BlockRange(
                        network=current_range.network,
                        start=current_range.start,
                        end=max(current_range.end, prev_range.end),
                        hash=prev_range.hash,
                    )
                    invalidation_ranges.append(invalidation)

        return invalidation_ranges

    def _is_duplicate_batch(self, current_ranges: List[BlockRange]) -> bool:
        """
        Check if all ranges in the current batch are duplicates of previous ranges.

        Args:
            current_ranges: Block ranges from the current batch

        Returns:
            True if all ranges are exact duplicates, False otherwise
        """
        if not current_ranges:
            return False

        # Check if all ranges in this batch are duplicates
        for current_range in current_ranges:
            prev_range = self.prev_ranges_by_network.get(current_range.network)

            # If we haven't seen this network before, it's not a duplicate
            if not prev_range:
                return False

            # If this range is different from the previous, it's not a duplicate batch
            if current_range != prev_range:
                return False

        # All ranges are exact duplicates
        return True

    def __enter__(self) -> 'ReorgAwareStream':
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        # Delegate to underlying stream
        if hasattr(self.stream_iterator, 'close'):
            self.stream_iterator.close()
