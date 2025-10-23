"""
Enhanced base class for data loaders with common functionality extracted from implementations.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import fields, is_dataclass
from datetime import datetime
from logging import Logger
from typing import Any, Dict, Generic, Iterator, List, Optional, Set, TypeVar

import pyarrow as pa

from ..streaming.checkpoint import CheckpointConfig, CheckpointState, NullCheckpointStore
from ..streaming.idempotency import (
    IdempotencyConfig,
    NullProcessedRangesStore,
    compute_batch_hash,
)
from ..streaming.resilience import (
    AdaptiveRateLimiter,
    BackPressureConfig,
    ErrorClassifier,
    ExponentialBackoff,
    RetryConfig,
)
from ..streaming.types import BlockRange, ResponseBatchWithReorg
from .types import LoadMode, LoadResult

# Type variable for configuration classes
TConfig = TypeVar('TConfig')


class DataLoader(ABC, Generic[TConfig]):
    """
    Enhanced abstract base class for all data loaders.

    Improvements:
    - Generic configuration type support
    - Built-in table tracking
    - Common batch processing logic
    - Standardized error handling
    - Connection state management
    - Schema conversion utilities
    """

    # Class-level attributes for loader metadata
    SUPPORTED_MODES: Set[LoadMode] = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH: bool = True
    SUPPORTS_TRANSACTIONS: bool = False

    def __init__(self, config: Dict[str, Any]) -> None:
        self.logger: Logger = logging.getLogger(f'{self.__class__.__name__}')
        self._connection: Optional[Any] = None
        self._is_connected: bool = False
        self._created_tables: Set[str] = set()  # Track created tables

        # Parse configuration into typed format
        self.config: TConfig = self._parse_config(config)

        # Validate configuration
        self._validate_config()

        # Initialize resilience components (enabled by default)
        resilience_config = config.get('resilience', {})
        self.retry_config = RetryConfig(**resilience_config.get('retry', {}))
        self.back_pressure_config = BackPressureConfig(**resilience_config.get('back_pressure', {}))

        self.rate_limiter = AdaptiveRateLimiter(self.back_pressure_config)

        # Initialize checkpointing (disabled by default for backward compatibility)
        checkpoint_config_dict = config.get('checkpoint', {})
        self.checkpoint_config = CheckpointConfig(**checkpoint_config_dict)
        # Start with NullCheckpointStore - will be replaced with DB store after connection
        self.checkpoint_store = NullCheckpointStore(self.checkpoint_config)

        # Initialize idempotency (disabled by default)
        idempotency_config_dict = config.get('idempotency', {})
        self.idempotency_config = IdempotencyConfig(**idempotency_config_dict)
        # Start with NullProcessedRangesStore - will be replaced with DB store after connection
        self.processed_ranges_store = NullProcessedRangesStore(self.idempotency_config)

    @property
    def is_connected(self) -> bool:
        """Check if the loader is connected to the target system."""
        return self._is_connected

    def _parse_config(self, config: Dict[str, Any]) -> TConfig:
        """
        Parse configuration into loader-specific format.
        Generic implementation that works with dataclass configs.
        Override only if you need custom parsing logic.
        """
        # For loaders that don't use typed configs, just return the dict
        if not hasattr(self, '__orig_bases__'):
            return config  # type: ignore

        # Filter out reserved config keys handled by base loader
        reserved_keys = {'resilience', 'checkpoint', 'idempotency'}
        filtered_config = {k: v for k, v in config.items() if k not in reserved_keys}

        # Get the actual config type from the generic parameter
        for base in self.__orig_bases__:
            if hasattr(base, '__args__') and base.__args__:
                config_type = base.__args__[0]
                # Check if it's a real type (not TypeVar)
                if hasattr(config_type, '__name__'):
                    try:
                        return config_type(**filtered_config)
                    except TypeError as e:
                        raise ValueError(f'Invalid {self.__class__.__name__} configuration: {e}') from e

        # Fallback for non-generic loaders
        return config  # type: ignore

    def _validate_config(self) -> None:
        """
        Validate configuration parameters.
        Override to add loader-specific validation.
        """
        required_fields = self._get_required_config_fields()

        # Handle both dict and dataclass config objects
        if is_dataclass(self.config):
            config_field_names = {f.name for f in fields(self.config)}
            missing_fields = [field for field in required_fields if field not in config_field_names]
        else:  # dict
            missing_fields = [field for field in required_fields if field not in self.config]

        if missing_fields:
            raise ValueError(f'Missing required configuration fields: {missing_fields}')

    def _get_required_config_fields(self) -> list[str]:
        """
        Return list of required configuration fields.
        Override in subclasses.
        """
        return []

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the target system"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the target system"""
        pass

    def close(self) -> None:
        """Alias for disconnect() for backward compatibility"""
        self.disconnect()

    @abstractmethod
    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """
        Implementation-specific batch loading logic.
        Returns number of rows loaded.
        """
        pass

    def load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """
        Load a single Arrow RecordBatch with automatic retry and back pressure.

        This method wraps _try_load_batch with resilience features:
        - Adaptive back pressure: Slow down on rate limits/timeouts
        - Exponential backoff: Retry transient failures with increasing delays
        """
        # Apply adaptive back pressure (rate limiting)
        self.rate_limiter.wait()

        # Retry loop with exponential backoff
        backoff = ExponentialBackoff(self.retry_config)
        last_error = None

        while True:
            # Attempt to load a batch
            result = self._try_load_batch(batch, table_name, **kwargs)

            if result.success:
                # Success path
                self.rate_limiter.record_success()
                return result

            # Failed - determine if we should retry
            last_error = result.error or 'Unknown error'
            is_transient = ErrorClassifier.is_transient(last_error)

            if not is_transient or not self.retry_config.enabled:
                # Permanent error or retry disabled - STOP THE CLIENT
                error_msg = (
                    f'FATAL: Permanent error loading batch (not retryable). '
                    f'Stopping client to prevent data loss. '
                    f'Error: {last_error}'
                )
                self.logger.error(error_msg)
                self.logger.error(
                    'Client will stop. On restart, streaming will resume from last checkpoint. '
                    'Fix the data/configuration issue before restarting.'
                )
                # Raise exception to stop the stream
                raise RuntimeError(error_msg)

            # Transient error - adapt rate limiter based on error type
            if '429' in last_error or 'rate limit' in last_error.lower():
                self.rate_limiter.record_rate_limit()
            elif 'timeout' in last_error.lower() or 'timed out' in last_error.lower():
                self.rate_limiter.record_timeout()

            # Calculate backoff delay
            delay = backoff.next_delay()
            if delay is None:
                # Max retries exceeded - STOP THE CLIENT
                error_msg = (
                    f'FATAL: Max retries ({self.retry_config.max_retries}) exceeded for batch. '
                    f'Stopping client to prevent data loss. '
                    f'Last error: {last_error}'
                )
                self.logger.error(error_msg)
                self.logger.error(
                    'Client will stop. On restart, streaming will resume from last checkpoint. '
                    'Fix the underlying issue before restarting.'
                )
                # Raise exception to stop the stream
                raise RuntimeError(error_msg)

            # Retry with backoff
            self.logger.warning(
                f'Transient error loading batch (attempt {backoff.attempt}/{self.retry_config.max_retries}): '
                f'{last_error}. Retrying in {delay:.1f}s...'
            )
            time.sleep(delay)

    def _try_load_batch(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> LoadResult:
        """
        Execute a single load attempt for an Arrow RecordBatch.

        This is called by load_batch() within the retry loop. It handles:
        - Connection management
        - Mode validation
        - Table creation
        - Error handling and timing
        - Metadata generation

        Returns a LoadResult indicating success or failure of this single attempt.
        """
        start_time = time.time()

        try:
            # Ensure connection
            if not self._is_connected:
                self.connect()

            # Validate mode
            mode = kwargs.get('mode', LoadMode.APPEND)
            if mode not in self.SUPPORTED_MODES:
                raise ValueError(f'Unsupported mode {mode}. Supported modes: {self.SUPPORTED_MODES}')

            # Handle table creation
            if kwargs.get('create_table', True) and table_name not in self._created_tables:
                if hasattr(self, '_create_table_from_schema'):
                    self._create_table_from_schema(batch.schema, table_name)
                    self._created_tables.add(table_name)
                else:
                    self.logger.warning(
                        f'Table {table_name} not created automatically. '
                        f'This loader does not yet implement "_create_table_from_schema". '
                        f'Please create any tables needed before running the loader. '
                    )

            # Handle overwrite mode
            if mode == LoadMode.OVERWRITE and hasattr(self, '_clear_table'):
                self._clear_table(table_name)

            # Perform the actual load
            rows_loaded = self._load_batch_impl(batch, table_name, **kwargs)

            duration = time.time() - start_time

            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2) if duration > 0 else 0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=True,
                metadata=self._get_batch_metadata(batch, duration, **kwargs),
            )

        except Exception as e:
            self.logger.error(f'Failed to load batch: {str(e)}')
            return LoadResult(
                rows_loaded=0,
                duration=time.time() - start_time,
                ops_per_second=0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e),
            )

    def load_table(self, table: pa.Table, table_name: str, **kwargs) -> LoadResult:
        """Load a complete Arrow Table with automatic batching"""
        start_time = time.time()
        batch_size = kwargs.get('batch_size', getattr(self, 'batch_size', 10000))

        rows_loaded = 0
        batch_count = 0
        errors = []

        try:
            # Process table in batches
            for batch in table.to_batches(max_chunksize=batch_size):
                result = self.load_batch(batch, table_name, **kwargs)

                if result.success:
                    rows_loaded += result.rows_loaded
                    batch_count += 1
                else:
                    errors.append(result.error)
                    if len(errors) > 5:  # Stop after too many errors
                        break

            duration = time.time() - start_time

            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=len(errors) == 0,
                error='; '.join(errors[:3]) if errors else None,
                metadata=self._get_table_metadata(table, duration, batch_count, **kwargs),
            )

        except Exception as e:
            self.logger.error(f'Failed to load table: {str(e)}')
            duration = time.time() - start_time
            return LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2) if duration > 0 else 0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e),
            )

    def load_stream(self, batch_iterator: Iterator[pa.RecordBatch], table_name: str, **kwargs) -> Iterator[LoadResult]:
        """Load data from a stream of batches"""
        if not self._is_connected:
            self.connect()

        rows_loaded = 0
        start_time = time.time()
        batch_count = 0

        try:
            for batch in batch_iterator:
                batch_count += 1
                result = self.load_batch(batch, table_name, **kwargs)

                if result.success:
                    rows_loaded += result.rows_loaded
                    self.logger.info(f'Loaded batch {batch_count}: {result.rows_loaded} rows in {result.duration:.2f}s')
                else:
                    self.logger.error(f'Failed to load batch {batch_count}: {result.error}')

                yield result

        except Exception as e:
            self.logger.error(f'Stream loading failed after {batch_count} batches: {str(e)}')
            duration = time.time() - start_time
            yield LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2),
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e),
                metadata={'batches_processed': batch_count},
            )

    def load_stream_continuous(
        self, stream_iterator: Iterator['ResponseBatchWithReorg'], table_name: str, **kwargs
    ) -> Iterator[LoadResult]:
        """
        Load data from a continuous streaming iterator with reorg support.

        This method orchestrates the streaming load process, delegating specific
        operations to focused helper methods for better maintainability.

        Args:
            stream_iterator: Iterator yielding ResponseBatchWithReorg objects
            table_name: Target table name
            **kwargs: Additional options (connection_name, worker_id, etc.)

        Yields:
            LoadResult for each batch or reorg event
        """
        if not self._is_connected:
            self.connect()

        rows_loaded = 0
        start_time = time.time()
        batch_count = 0
        reorg_count = 0
        connection_name = kwargs.get('connection_name', 'unknown')
        worker_id = kwargs.get('worker_id', 0)

        try:
            for response in stream_iterator:
                if response.is_reorg:
                    # Process reorganization event
                    reorg_count += 1
                    result = self._process_reorg_event(
                        response, table_name, connection_name, reorg_count, start_time, worker_id
                    )
                    yield result

                else:
                    # Process normal data batch
                    batch_count += 1

                    # Prepare batch data
                    batch_data = response.data.data
                    if response.data.metadata.ranges:
                        batch_data = self._add_metadata_columns(batch_data, response.data.metadata.ranges)

                    # Compute batch hash if verification enabled
                    batch_hash = None
                    if self.idempotency_config.verification_hash:
                        batch_hash = compute_batch_hash(batch_data)

                    # Choose processing strategy: transactional vs non-transactional
                    use_transactional = (
                        hasattr(self, 'load_batch_transactional')
                        and self.idempotency_config.enabled
                        and response.data.metadata.ranges
                    )

                    if use_transactional:
                        # Atomic transactional loading (PostgreSQL with idempotency)
                        result = self._process_batch_transactional(
                            batch_data,
                            table_name,
                            connection_name,
                            response.data.metadata.ranges,
                            batch_hash,
                        )
                    else:
                        # Non-transactional loading (separate check, load, mark)
                        # Filter out parameters we've already extracted from kwargs
                        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ('connection_name', 'worker_id')}
                        result = self._process_batch_non_transactional(
                            batch_data,
                            table_name,
                            connection_name,
                            response.data.metadata.ranges,
                            batch_hash,
                            **filtered_kwargs,
                        )

                        # Handle skip case (duplicate detected in non-transactional flow)
                        if result and result.metadata.get('operation') == 'skip_duplicate':
                            yield result
                            continue

                    # Update total rows loaded
                    if result and result.success:
                        rows_loaded += result.rows_loaded

                    # Save checkpoint when microbatch completes
                    if response.data.metadata.ranges:
                        self._save_checkpoint_if_complete(
                            response.data.metadata.ranges,
                            response.data.metadata.ranges_complete,
                            connection_name,
                            table_name,
                            worker_id,
                            batch_count,
                        )

                    # Augment result with streaming metadata and yield
                    if result:
                        result = self._augment_streaming_result(
                            result, batch_count, response.data.metadata.ranges, response.data.metadata.ranges_complete
                        )
                        yield result

        except KeyboardInterrupt:
            self.logger.info(f'Streaming cancelled by user after {batch_count} batches, {rows_loaded} rows loaded')
            raise
        except Exception as e:
            self.logger.error(f'Streaming failed after {batch_count} batches: {str(e)}')
            duration = time.time() - start_time
            yield LoadResult(
                rows_loaded=rows_loaded,
                duration=duration,
                ops_per_second=round(rows_loaded / duration, 2) if duration > 0 else 0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e),
                metadata={
                    'batches_processed': batch_count,
                    'reorgs_processed': reorg_count,
                    'is_streaming': True,
                },
            )

    def _process_reorg_event(
        self,
        response: 'ResponseBatchWithReorg',
        table_name: str,
        connection_name: str,
        reorg_count: int,
        start_time: float,
        worker_id: int = 0,
    ) -> LoadResult:
        """
        Process a reorganization event.

        Args:
            response: Response containing invalidation ranges
            table_name: Target table name
            connection_name: Connection identifier
            reorg_count: Number of reorgs processed so far
            start_time: Stream start time for duration calculation

        Returns:
            LoadResult for the reorg event
        """
        try:
            # Let the loader implementation handle the reorg (rollback data)
            self._handle_reorg(response.invalidation_ranges, table_name)

            # Create reorg checkpoint marking the resume point
            if response.invalidation_ranges:
                # Compute resume point from invalidation ranges
                reorg_ranges = self._compute_reorg_resume_point(response.invalidation_ranges)

                # Log reorg details
                for range_obj in response.invalidation_ranges:
                    self.logger.warning(
                        f'Reorg detected on {range_obj.network}: blocks {range_obj.start}-{range_obj.end} invalidated'
                    )

                # Save reorg checkpoint (keeps old checkpoints for history)
                checkpoint = CheckpointState(
                    ranges=reorg_ranges,
                    timestamp=datetime.utcnow(),
                    worker_id=worker_id,
                    is_reorg=True,
                )
                self.checkpoint_store.save(connection_name, table_name, checkpoint)

                self.logger.info(
                    f'Saved reorg checkpoint. Will resume from: '
                    f'{", ".join(f"{r.network}:{r.start}" for r in reorg_ranges)}'
                )

            # Build and return reorg result
            duration = time.time() - start_time
            return LoadResult(
                rows_loaded=0,
                duration=duration,
                ops_per_second=0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=True,
                is_reorg=True,
                invalidation_ranges=response.invalidation_ranges,
                metadata={
                    'operation': 'reorg',
                    'invalidation_count': len(response.invalidation_ranges or []),
                    'reorg_number': reorg_count,
                },
            )

        except Exception as e:
            self.logger.error(f'Failed to handle reorg: {str(e)}')
            raise

    def _process_batch_transactional(
        self,
        batch_data: pa.RecordBatch,
        table_name: str,
        connection_name: str,
        ranges: List[BlockRange],
        batch_hash: Optional[str],
    ) -> LoadResult:
        """
        Process a data batch using transactional exactly-once semantics.

        Performs atomic check + load + mark in a single database transaction.

        Args:
            batch_data: Arrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier
            ranges: Block ranges for this batch
            batch_hash: Optional hash for verification

        Returns:
            LoadResult with operation outcome
        """
        start_time = time.time()
        try:
            rows_loaded_batch = self.load_batch_transactional(
                batch_data, table_name, connection_name, ranges, batch_hash
            )
            duration = time.time() - start_time

            return LoadResult(
                rows_loaded=rows_loaded_batch,
                duration=duration,
                ops_per_second=round(rows_loaded_batch / duration, 2) if duration > 0 else 0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=True,
                metadata={
                    'operation': 'transactional_load' if rows_loaded_batch > 0 else 'skip_duplicate',
                    'ranges': [r.to_dict() for r in ranges],
                },
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f'Transactional batch load failed: {e}')
            return LoadResult(
                rows_loaded=0,
                duration=duration,
                ops_per_second=0,
                table_name=table_name,
                loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                success=False,
                error=str(e),
            )

    def _process_batch_non_transactional(
        self,
        batch_data: pa.RecordBatch,
        table_name: str,
        connection_name: str,
        ranges: Optional[List[BlockRange]],
        batch_hash: Optional[str],
        **kwargs,
    ) -> Optional[LoadResult]:
        """
        Process a data batch using non-transactional flow (separate check, load, mark).

        Used when loader doesn't support transactions or idempotency is disabled.

        Args:
            batch_data: Arrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier
            ranges: Block ranges for this batch (if available)
            batch_hash: Optional hash for verification
            **kwargs: Additional options passed to load_batch

        Returns:
            LoadResult, or None if batch was skipped as duplicate
        """
        # Check if batch already processed (idempotency / exactly-once)
        if ranges:
            is_duplicate = self.processed_ranges_store.is_processed(connection_name, table_name, ranges)

            if is_duplicate:
                # Skip this batch - already processed
                self.logger.info(f'Skipping duplicate batch: {len(ranges)} ranges already processed for {table_name}')
                return LoadResult(
                    rows_loaded=0,
                    duration=0.0,
                    ops_per_second=0.0,
                    table_name=table_name,
                    loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                    success=True,
                    metadata={'operation': 'skip_duplicate', 'ranges': [r.to_dict() for r in ranges]},
                )

        # Load batch
        result = self.load_batch(batch_data, table_name, **kwargs)

        if result.success and ranges:
            # Mark batch as processed (for exactly-once semantics)
            try:
                self.processed_ranges_store.mark_processed(connection_name, table_name, ranges, batch_hash)
            except Exception as e:
                self.logger.error(f'Failed to mark ranges as processed: {e}')
                # Continue anyway - checkpoint will provide resume capability

        return result

    def _save_checkpoint_if_complete(
        self,
        ranges: List[BlockRange],
        ranges_complete: bool,
        connection_name: str,
        table_name: str,
        worker_id: int,
        batch_count: int,
    ) -> None:
        """
        Save checkpoint when server signals microbatch completion.

        Args:
            ranges: Block ranges for this batch
            ranges_complete: Whether this completes a microbatch
            connection_name: Connection identifier
            table_name: Target table name
            worker_id: Worker ID for multi-worker setups
            batch_count: Current batch number
        """
        if ranges_complete and ranges:
            from datetime import datetime

            checkpoint = CheckpointState(
                ranges=ranges,
                timestamp=datetime.utcnow(),
                worker_id=worker_id,
            )

            try:
                self.checkpoint_store.save(connection_name, table_name, checkpoint)
                self.logger.info(f'Saved checkpoint at batch {batch_count} ({len(checkpoint.ranges)} ranges)')
            except Exception as e:
                # Log but don't fail the stream
                self.logger.error(f'Failed to save checkpoint: {e}')

    def _augment_streaming_result(
        self, result: LoadResult, batch_count: int, ranges: Optional[List[BlockRange]], ranges_complete: bool
    ) -> LoadResult:
        """
        Add streaming-specific metadata to a load result.

        Args:
            result: LoadResult to augment
            batch_count: Current batch number
            ranges: Block ranges for this batch (if available)
            ranges_complete: Whether this completes a microbatch

        Returns:
            Augmented LoadResult
        """
        result.metadata['is_streaming'] = True
        result.metadata['batch_count'] = batch_count
        result.metadata['ranges_complete'] = ranges_complete
        if ranges:
            result.metadata['block_ranges'] = [{'network': r.network, 'start': r.start, 'end': r.end} for r in ranges]
        return result

    def _compute_reorg_resume_point(self, invalidation_ranges: List[BlockRange]) -> List[BlockRange]:
        """
        Compute the resume point after a reorg from invalidation ranges.

        For each network in invalidation_ranges, the resume point is the start
        of the earliest invalidated block (the first block that needs reprocessing).

        Args:
            invalidation_ranges: List of block ranges that were invalidated by the reorg

        Returns:
            List of BlockRange objects representing the resume point for each affected network
        """
        # Group by network and find minimum start block
        network_min_blocks = {}
        for range_obj in invalidation_ranges:
            network = range_obj.network
            if network not in network_min_blocks or range_obj.start < network_min_blocks[network]:
                network_min_blocks[network] = range_obj.start

        # Create resume ranges - we resume FROM the invalidated start block
        resume_ranges = []
        for network, start_block in network_min_blocks.items():
            resume_ranges.append(
                BlockRange(
                    network=network,
                    start=start_block,
                    end=start_block,  # Single point - the resume block
                )
            )

        return resume_ranges

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str) -> None:
        """
        Handle a blockchain reorganization by invalidating affected data.

        This method should be implemented by each loader to handle reorgs
        in a way appropriate to their storage backend.

        Args:
            invalidation_ranges: List of block ranges to invalidate
            table_name: The table containing the data to invalidate

        Raises:
            NotImplementedError: If the loader doesn't support reorg handling
        """
        raise NotImplementedError(
            f'{self.__class__.__name__} does not implement _handle_reorg(). '
            'Streaming with reorg detection requires implementing this method.'
        )

    def _add_metadata_columns(self, data: pa.RecordBatch, block_ranges: List[BlockRange]) -> pa.RecordBatch:
        """
        Add metadata columns for streaming data with multi-network blockchain information.

        Adds metadata column:
        - _meta_block_ranges: JSON array of all block ranges for cross-network support

        This approach supports multi-network scenarios like bridge monitoring, cross-chain
        DEX aggregation, and multi-network governance tracking. Each loader can optimize
        storage (e.g., PostgreSQL can use JSONB with GIN indexing or native arrays).

        Args:
            data: The original Arrow RecordBatch
            block_ranges: List of BlockRange objects associated with this batch

        Returns:
            Arrow RecordBatch with metadata columns added
        """
        if not block_ranges:
            return data

        # Create JSON representation of all block ranges for multi-network support
        import json

        ranges_json = json.dumps([{'network': br.network, 'start': br.start, 'end': br.end} for br in block_ranges])

        # Create metadata array
        num_rows = len(data)
        ranges_array = pa.array([ranges_json] * num_rows, type=pa.string())

        # Add metadata column
        result = data.append_column('_meta_block_ranges', ranges_array)

        return result

    def _get_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get standard metadata for batch operations"""
        metadata = {
            'operation': 'load_batch',  # Required by tests
            'batch_size': batch.num_rows,
            'schema_fields': len(batch.schema),
            'throughput_rows_per_sec': round(batch.num_rows / duration, 2) if duration > 0 else 0,
        }

        # Add any kwargs that might be useful as metadata
        for key in ['mode', 'batch_size', 'table_name']:
            if key in kwargs:
                metadata[key] = kwargs[key]

        # Allow loaders to add their specific metadata
        loader_metadata = self._get_loader_batch_metadata(batch, duration, **kwargs)
        metadata.update(loader_metadata)

        return metadata

    def _get_table_metadata(self, table: pa.Table, duration: float, batch_count: int, **kwargs) -> Dict[str, Any]:
        """Get standard metadata for table operations"""
        metadata = {
            'operation': 'load_table',
            'batches_processed': batch_count,
            'rows_loaded': table.num_rows,
            'columns': len(table.schema),
            'avg_batch_size': round(table.num_rows / batch_count, 2) if batch_count > 0 else 0,
            'table_size_bytes': table.nbytes,
            'table_size_mb': round(table.nbytes / 1024 / 1024, 2),
            'throughput_rows_per_sec': round(table.num_rows / duration, 2) if duration > 0 else 0,
        }

        # Allow loaders to add their specific metadata
        loader_metadata = self._get_loader_table_metadata(table, duration, batch_count, **kwargs)
        metadata.update(loader_metadata)

        return metadata

    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Override in subclasses to add loader-specific batch metadata"""
        return {}

    def _get_loader_table_metadata(
        self, table: pa.Table, duration: float, batch_count: int, **kwargs
    ) -> Dict[str, Any]:
        """Override in subclasses to add loader-specific table metadata"""
        return {}

    def __enter__(self) -> 'DataLoader':
        self.connect()
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> None:
        self.disconnect()

    # Optional methods that subclasses can implement
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a table in the target system"""
        raise NotImplementedError(f'{self.__class__.__name__} does not implement get_table_info()')

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get the schema of an existing table"""
        raise NotImplementedError(f'{self.__class__.__name__} does not implement get_table_schema()')

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the target system"""
        try:
            info = self.get_table_info(table_name)
            return info is not None
        except NotImplementedError:
            return table_name in self._created_tables

    def health_check(self) -> Dict[str, Any]:
        """Perform a health check on the connection"""
        return {'healthy': self._is_connected, 'loader_type': self.__class__.__name__}
