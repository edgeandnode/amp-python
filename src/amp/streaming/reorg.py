"""
Reorg-aware streaming wrapper that detects blockchain reorganizations.
"""

import logging
from typing import Dict, Iterator, List

from .iterator import StreamingResultIterator
from .types import BlockRange, ResponseBatch


class ReorgAwareStream:
    """
    Wraps a streaming result iterator to detect and signal blockchain reorganizations.

    This class monitors the block ranges in consecutive batches to detect chain
    reorganizations (reorgs). When a reorg is detected, a ResponseBatch with
    is_reorg=True is emitted containing the invalidation ranges.
    """

    def __init__(self, stream_iterator: StreamingResultIterator):
        """
        Initialize the reorg-aware stream.

        Args:
            stream_iterator: The underlying streaming result iterator
        """
        self.stream_iterator = stream_iterator
        # Track the latest range for each network
        self.prev_ranges_by_network: Dict[str, BlockRange] = {}
        self.logger = logging.getLogger(__name__)

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

        A reorg is detected when:
        - A range starts at or before the end of the previous range for the same network
        - The range is different from the previous range

        Args:
            current_ranges: Block ranges from the current batch

        Returns:
            List of block ranges that should be invalidated due to reorg
        """
        invalidation_ranges = []

        for current_range in current_ranges:
            # Get the previous range for this network
            prev_range = self.prev_ranges_by_network.get(current_range.network)

            if prev_range:
                # Check if this indicates a reorg
                if current_range != prev_range and current_range.start <= prev_range.end:
                    # Reorg detected - create invalidation range
                    # Invalidate from the start of the current range to the max end
                    invalidation = BlockRange(
                        network=current_range.network,
                        start=current_range.start,
                        end=max(current_range.end, prev_range.end),
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
