"""
Streaming result iterator for continuous data loading.
"""

import logging
import signal
from typing import Iterator, Optional, Tuple

import pyarrow as pa
from pyarrow import flight

from .types import BatchMetadata, ResponseBatch


class StreamingResultIterator:
    """
    Iterator that yields ResponseBatch objects from a streaming Flight SQL query.

    This iterator handles the decoding of Flight data streams and extraction
    of metadata from each batch.
    """

    def __init__(self, flight_reader: flight.FlightStreamReader):
        """
        Initialize the streaming iterator.

        Args:
            flight_reader: PyArrow Flight stream reader
        """
        self.flight_reader = flight_reader
        self.logger = logging.getLogger(__name__)
        self._closed = False

        signal.signal(signal.SIGINT, self._handle_interrupt)

    def __iter__(self) -> Iterator[ResponseBatch]:
        """Return iterator instance"""
        return self

    def close(self):
        """Close the stream"""
        if not self._closed:
            self.logger.info('Closing stream')
            self._closed = True
        try:
            self.flight_reader.cancel()
        except Exception as e:
            self.logger.warning(f'Error cancelling flight reader: {e}')

    def _handle_interrupt(self, signum, frame):
        """Handle SIGINT (Ctrl+C) signal"""
        self.logger.info('Interrupt signal received (%s), cancelling stream...', signum)
        self.close()

    def __next__(self) -> ResponseBatch:
        """
        Get the next batch from the stream.

        Returns:
            ResponseBatch containing data and metadata

        Raises:
            StopIteration: When stream is exhausted
            KeyboardInterrupt: When user cancels the stream
        """
        if self._closed:
            raise StopIteration('Stream has been closed')

        try:
            # Read next batch from Flight stream
            batch, metadata = self._read_next_batch()

            if batch is None:
                self._closed = True
                raise StopIteration()

            return ResponseBatch(data=batch, metadata=metadata)

        except KeyboardInterrupt:
            self.logger.info('Stream cancelled by user')
            self.close()
            raise
        except StopIteration:
            self.close()
            raise
        except Exception as e:
            self.logger.error(f'Error reading from stream: {e}')
            self.close()
            raise

    def _read_next_batch(self) -> Tuple[Optional[pa.RecordBatch], Optional[BatchMetadata]]:
        """
        Read the next batch and metadata from the Flight stream.

        Returns:
            Tuple of (batch, metadata) or (None, None) if stream is exhausted
        """
        try:
            # PyArrow's FlightStreamReader provides batches via iteration
            chunk = next(self.flight_reader)

            # Extract and parse metadata if available
            metadata = BatchMetadata(ranges=[])
            if hasattr(chunk, 'app_metadata') and chunk.app_metadata:
                try:
                    metadata = BatchMetadata.from_flight_data(chunk.app_metadata)
                except Exception as e:
                    self.logger.warning(f'Failed to parse batch metadata: {e}')

            return chunk.data, metadata

        except StopIteration:
            return None, None

    def __enter__(self) -> 'StreamingResultIterator':
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()
