"""
Enhanced base class for data loaders with common functionality extracted from implementations.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import fields, is_dataclass
from logging import Logger
from typing import Any, Dict, Generic, Iterator, List, Optional, Set, TypeVar

import pyarrow as pa

from ..streaming.resilience import (
    AdaptiveRateLimiter,
    BackPressureConfig,
    ErrorClassifier,
    ExponentialBackoff,
    RetryConfig,
)
from ..streaming.state import BatchIdentifier, InMemoryStreamStateStore, NullStreamStateStore
from ..streaming.types import BlockRange, ResponseBatch
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

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        self.logger: Logger = logging.getLogger(f'{self.__class__.__name__}')
        self._connection: Optional[Any] = None
        self._is_connected: bool = False
        self._created_tables: Set[str] = set()  # Track created tables
        self.label_manager = label_manager  # For CSV label joining

        # Parse configuration into typed format
        self.config: TConfig = self._parse_config(config)

        # Validate configuration
        self._validate_config()

        # Initialize resilience components (enabled by default)
        resilience_config = config.get('resilience', {})
        self.retry_config = RetryConfig(**resilience_config.get('retry', {}))
        self.back_pressure_config = BackPressureConfig(**resilience_config.get('back_pressure', {}))

        self.rate_limiter = AdaptiveRateLimiter(self.back_pressure_config)

        # Initialize unified stream state management (enabled by default with in-memory storage)
        state_config_dict = config.get('state', {})
        self.state_enabled = state_config_dict.get('enabled', True)
        self.state_storage = state_config_dict.get('storage', 'memory')
        self.store_batch_id = state_config_dict.get('store_batch_id', True)
        self.store_full_metadata = state_config_dict.get('store_full_metadata', False)

        # Start with in-memory or null store - loaders can replace with DB store after connection
        if self.state_enabled:
            self.state_store = InMemoryStreamStateStore()
        else:
            self.state_store = NullStreamStateStore()

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
        reserved_keys = {'resilience', 'state', 'checkpoint', 'idempotency'}  # Keep old keys for backward compat
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
        - Label joining (if configured)
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

            # Apply label joining if requested
            label_config = kwargs.pop('label_config', None)
            if label_config:
                # Perform the join
                batch = self._join_with_labels(
                    batch, label_config.label_name, label_config.label_key_column, label_config.stream_key_column
                )
                self.logger.debug(
                    f'Joined batch with label {label_config.label_name}: {batch.num_rows} rows after join '
                    f'(columns: {", ".join(batch.schema.names)})'
                )

                # Skip empty batches after label join (all rows filtered out)
                if batch.num_rows == 0:
                    self.logger.info(f'Skipping batch: 0 rows after label join with {label_config.label_name}')
                    return LoadResult(
                        rows_loaded=0,
                        duration=time.time() - start_time,
                        ops_per_second=0,
                        table_name=table_name,
                        loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                        success=True,
                        metadata={'skipped_empty_batch': True, 'label_join_filtered': True},
                    )

            # Add metadata columns if block_ranges provided (enables reorg handling for non-streaming loads)
            block_ranges = kwargs.pop('block_ranges', None)
            connection_name = kwargs.pop('connection_name', 'default')
            if block_ranges:
                batch = self._add_metadata_columns(batch, block_ranges)
                self.logger.debug(
                    f'Added metadata columns for {len(block_ranges)} block ranges '
                    f'(columns: {", ".join(batch.schema.names)})'
                )

            # Handle table creation (use joined schema if applicable)
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

            # Handle overwrite mode (only if not already cleared by load_table)
            if mode == LoadMode.OVERWRITE and not kwargs.get('_already_cleared') and hasattr(self, '_clear_table'):
                self._clear_table(table_name)

            # Perform the actual load
            rows_loaded = self._load_batch_impl(batch, table_name, **kwargs)

            # Track batch in state store if block_ranges were provided
            if block_ranges and self.state_enabled:
                try:
                    batch_ids = [BatchIdentifier.from_block_range(br) for br in block_ranges]
                    self.state_store.mark_processed(connection_name, table_name, batch_ids)
                    self.logger.debug(f'Tracked {len(batch_ids)} batches in state store for reorg handling')
                except Exception as e:
                    self.logger.warning(f'Failed to track batches in state store: {e}')

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

        # Handle overwrite mode ONCE before processing batches
        mode = kwargs.get('mode', LoadMode.APPEND)
        if mode == LoadMode.OVERWRITE and hasattr(self, '_clear_table'):
            self._clear_table(table_name)
            # Prevent subsequent batch loads from clearing again
            kwargs = kwargs.copy()
            kwargs['_already_cleared'] = True

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
                metadata=self._get_table_metadata(table, duration, batch_count, table_name=table_name, **kwargs),
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
        self, stream_iterator: Iterator['ResponseBatch'], table_name: str, **kwargs
    ) -> Iterator[LoadResult]:
        """
        Load data from a continuous streaming iterator with reorg support.

        This method orchestrates the streaming load process, delegating specific
        operations to focused helper methods for better maintainability.

        Args:
            stream_iterator: Iterator yielding ResponseBatch objects
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
                    batch_data = response.data
                    if response.metadata.ranges:
                        batch_data = self._add_metadata_columns(batch_data, response.metadata.ranges)

                    # Choose processing strategy: transactional vs non-transactional
                    use_transactional = (
                        hasattr(self, 'load_batch_transactional') and self.state_enabled and response.metadata.ranges
                    )

                    if use_transactional:
                        # Atomic transactional loading (PostgreSQL with state management)
                        result = self._process_batch_transactional(
                            batch_data,
                            table_name,
                            connection_name,
                            response.metadata.ranges,
                            ranges_complete=response.metadata.ranges_complete,
                        )
                    else:
                        # Non-transactional loading (separate check, load, mark)
                        # Filter out parameters we've already extracted from kwargs
                        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in ('connection_name', 'worker_id')}
                        result = self._process_batch_non_transactional(
                            batch_data,
                            table_name,
                            connection_name,
                            response.metadata.ranges,
                            ranges_complete=response.metadata.ranges_complete,
                            **filtered_kwargs,
                        )

                        # Handle skip case (duplicate detected in non-transactional flow)
                        if result and result.metadata.get('operation') == 'skip_duplicate':
                            yield result
                            continue

                    # Update total rows loaded
                    if result and result.success:
                        rows_loaded += result.rows_loaded

                    # State is automatically updated via mark_processed in batch processing methods
                    # No separate checkpoint saving needed with unified StreamState

                    # Augment result with streaming metadata and yield
                    if result:
                        result = self._augment_streaming_result(
                            result, batch_count, response.metadata.ranges, response.metadata.ranges_complete
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
        response: 'ResponseBatch',
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
            self._handle_reorg(response.invalidation_ranges, table_name, connection_name)

            # Invalidate affected batches from state store
            if response.invalidation_ranges:
                # Log reorg details
                for range_obj in response.invalidation_ranges:
                    self.logger.warning(
                        f'Reorg detected on {range_obj.network}: blocks {range_obj.start}-{range_obj.end} invalidated'
                    )

                    # Invalidate batches in state store
                    try:
                        invalidated_batch_ids = self.state_store.invalidate_from_block(
                            connection_name, table_name, range_obj.network, range_obj.start
                        )
                        self.logger.info(
                            f'Invalidated {len(invalidated_batch_ids)} batches from state store for '
                            f'{range_obj.network} from block {range_obj.start}'
                        )
                    except Exception as e:
                        self.logger.error(f'Failed to invalidate batches from state store: {e}')

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
        ranges_complete: bool = False,
    ) -> LoadResult:
        """
        Process a data batch using transactional exactly-once semantics.

        Performs atomic check + load + mark in a single database transaction.

        Args:
            batch_data: Arrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier
            ranges: Block ranges for this batch
            ranges_complete: True when this RecordBatch completes a microbatch (streaming only)

        Returns:
            LoadResult with operation outcome
        """
        start_time = time.time()
        try:
            # Delegate to loader-specific transactional implementation
            # Loaders that support transactions implement load_batch_transactional()
            rows_loaded_batch = self.load_batch_transactional(
                batch_data, table_name, connection_name, ranges, ranges_complete
            )
            duration = time.time() - start_time

            # Mark batches as processed ONLY when microbatch is complete
            # multiple RecordBatches can share the same microbatch ID
            if ranges and ranges_complete:
                batch_ids = [BatchIdentifier.from_block_range(br) for br in ranges]
                self.state_store.mark_processed(connection_name, table_name, batch_ids)
                self.logger.debug(f'Marked microbatch as processed: {len(batch_ids)} batch IDs')

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
                    'ranges_complete': ranges_complete,
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
        ranges_complete: bool = False,
        **kwargs,
    ) -> Optional[LoadResult]:
        """
        Process a data batch using non-transactional flow (separate check, load, mark).

        Used when loader doesn't support transactions or state management is disabled.

        Args:
            batch_data: Arrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier
            ranges: Block ranges for this batch (if available)
            ranges_complete: True when this RecordBatch completes a microbatch (streaming only)
            **kwargs: Additional options passed to load_batch

        Returns:
            LoadResult, or None if batch was skipped as duplicate
        """
        # Check if batch already processed (idempotency / exactly-once)
        # For streaming: only check when ranges_complete=True (end of microbatch)
        # Multiple RecordBatches can share the same microbatch ID, so we must wait
        # until the entire microbatch is delivered before checking/marking as processed
        if ranges and self.state_enabled and ranges_complete:
            try:
                batch_ids = [BatchIdentifier.from_block_range(br) for br in ranges]
                is_duplicate = self.state_store.is_processed(connection_name, table_name, batch_ids)

                if is_duplicate:
                    # Skip this batch - already processed
                    self.logger.info(
                        f'Skipping duplicate microbatch: {len(ranges)} ranges already processed for {table_name}'
                    )
                    return LoadResult(
                        rows_loaded=0,
                        duration=0.0,
                        ops_per_second=0.0,
                        table_name=table_name,
                        loader_type=self.__class__.__name__.replace('Loader', '').lower(),
                        success=True,
                        metadata={'operation': 'skip_duplicate', 'ranges': [r.to_dict() for r in ranges]},
                    )
            except ValueError as e:
                # BlockRange missing hash - log and continue without idempotency check
                self.logger.warning(f'Cannot check for duplicates: {e}. Processing batch anyway.')

        # Load batch (always load, even if part of larger microbatch)
        result = self.load_batch(batch_data, table_name, **kwargs)

        # Mark batch as processed ONLY when microbatch is complete
        # This ensures we don't skip subsequent RecordBatches within the same microbatch
        if result.success and ranges and self.state_enabled and ranges_complete:
            try:
                batch_ids = [BatchIdentifier.from_block_range(br) for br in ranges]
                self.state_store.mark_processed(connection_name, table_name, batch_ids)
                self.logger.debug(f'Marked microbatch as processed: {len(batch_ids)} batch IDs')
            except Exception as e:
                self.logger.error(f'Failed to mark batches as processed: {e}')
                # Continue anyway - state store provides resume capability

        return result

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

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str, connection_name: str) -> None:
        """
        Handle a blockchain reorganization by invalidating affected data.

        This method should be implemented by each loader to handle reorgs
        in a way appropriate to their storage backend. The loader should delete
        data rows that match the invalidated batch IDs.

        Args:
            invalidation_ranges: List of block ranges to invalidate
            table_name: The table containing the data to invalidate
            connection_name: Connection identifier for state lookup

        Raises:
            NotImplementedError: If the loader doesn't support reorg handling
        """
        raise NotImplementedError(
            f'{self.__class__.__name__} does not implement _handle_reorg(). '
            'Streaming with reorg detection requires implementing this method.'
        )

    def _add_metadata_columns(self, data: pa.RecordBatch, block_ranges: List[BlockRange]) -> pa.RecordBatch:
        """
        Add metadata columns for streaming data with compact batch identification.

        Adds metadata columns:
        - _amp_batch_id: Compact unique identifier (16 hex chars) for fast indexing
        - _amp_block_ranges: Optional full JSON for debugging (if store_full_metadata=True)

        The batch_id is a hash of (network, start, end, block_hash) making it unique
        across blockchain reorganizations. This enables:
        - Fast reorg invalidation via indexed DELETE WHERE batch_id IN (...)
        - 85-90% reduction in metadata storage vs full JSON
        - Consistent batch identity across checkpoint and data tables

        Args:
            data: The original Arrow RecordBatch
            block_ranges: List of BlockRange objects associated with this batch

        Returns:
            Arrow RecordBatch with metadata columns added
        """
        if not block_ranges:
            return data

        num_rows = len(data)
        result = data

        # Add compact batch_id column (primary metadata)
        # BatchIdentifier handles both hash-based (streaming) and position-based (parallel) IDs
        if self.store_batch_id:
            # Convert BlockRanges to BatchIdentifiers and get compact unique IDs
            batch_ids = [BatchIdentifier.from_block_range(br) for br in block_ranges]
            # Combine multiple batch IDs with "|" separator for multi-network batches
            batch_id_str = '|'.join(bid.unique_id for bid in batch_ids)
            batch_id_array = pa.array([batch_id_str] * num_rows, type=pa.string())
            result = result.append_column('_amp_batch_id', batch_id_array)

        # Optionally add full JSON for debugging/auditing
        if self.store_full_metadata:
            import json

            ranges_json = json.dumps(
                [
                    {
                        'network': br.network,
                        'start': br.start,
                        'end': br.end,
                        'hash': br.hash,
                        'prev_hash': br.prev_hash,
                    }
                    for br in block_ranges
                ]
            )
            ranges_array = pa.array([ranges_json] * num_rows, type=pa.string())
            result = result.append_column('_amp_block_ranges', ranges_array)

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

    def _get_effective_schema(
        self, original_schema: pa.Schema, label_name: Optional[str], label_key_column: Optional[str]
    ) -> pa.Schema:
        """
        Get effective schema by merging label columns into original schema.

        If label_name is None, returns original schema unchanged.
        Otherwise, merges label columns (excluding the join key which is already in original).

        Args:
            original_schema: Original data schema
            label_name: Name of the label dataset (None if no labels)
            label_key_column: Column name in the label table to join on

        Returns:
            Schema with label columns merged in
        """
        if label_name is None or label_key_column is None:
            return original_schema

        if self.label_manager is None:
            raise ValueError('Label manager not configured')

        label_table = self.label_manager.get_label(label_name)
        if label_table is None:
            raise ValueError(f"Label '{label_name}' not found")

        # Start with original schema fields
        merged_fields = list(original_schema)

        # Add label columns (excluding the join key which is already in original)
        for field in label_table.schema:
            if field.name != label_key_column and field.name not in original_schema.names:
                merged_fields.append(field)

        return pa.schema(merged_fields)

    def _join_with_labels(
        self, batch: pa.RecordBatch, label_name: str, label_key_column: str, stream_key_column: str
    ) -> pa.RecordBatch:
        """
        Join batch data with labels using inner join.

        Handles automatic type conversion between stream and label key columns
        (e.g., string ↔ binary for Ethereum addresses).

        Args:
            batch: Original data batch
            label_name: Name of the label dataset
            label_key_column: Column name in the label table to join on
            stream_key_column: Column name in the batch data to join on

        Returns:
            Joined RecordBatch with label columns added

        Raises:
            ValueError: If label_manager not configured, label not found, or invalid columns
        """
        import sys
        import time

        t_start = time.perf_counter()

        if self.label_manager is None:
            raise ValueError('Label manager not configured')

        label_table = self.label_manager.get_label(label_name)
        if label_table is None:
            raise ValueError(f"Label '{label_name}' not found")

        # Validate columns exist
        if stream_key_column not in batch.schema.names:
            raise ValueError(f"Stream key column '{stream_key_column}' not found in batch schema")

        if label_key_column not in label_table.schema.names:
            raise ValueError(f"Label key column '{label_key_column}' not found in label table")

        # Convert batch to table for join operation
        batch_table = pa.Table.from_batches([batch])
        input_rows = batch_table.num_rows

        # Get column types for join keys
        stream_key_type = batch_table.schema.field(stream_key_column).type
        label_key_type = label_table.schema.field(label_key_column).type

        # If types don't match, cast one to match the other
        # Prefer casting to binary since that's more efficient

        type_conversion_time_ms = 0.0
        if stream_key_type != label_key_type:
            t_conversion_start = time.perf_counter()

            # Try to cast stream key to label key type
            if pa.types.is_fixed_size_binary(label_key_type) and pa.types.is_string(stream_key_type):
                # Cast string to binary (hex strings like "0xABCD...")
                def hex_to_binary(value):
                    if value is None:
                        return None
                    # Remove 0x prefix if present
                    hex_str = value[2:] if value.startswith('0x') else value
                    return bytes.fromhex(hex_str)

                # Cast the stream column to binary
                stream_column = batch_table.column(stream_key_column)
                binary_length = label_key_type.byte_width
                binary_values = pa.array(
                    [hex_to_binary(v.as_py()) for v in stream_column], type=pa.binary(binary_length)
                )
                batch_table = batch_table.set_column(
                    batch_table.schema.get_field_index(stream_key_column), stream_key_column, binary_values
                )
            elif pa.types.is_binary(stream_key_type) and pa.types.is_string(label_key_type):
                # Cast binary to string (for test compatibility)
                stream_column = batch_table.column(stream_key_column)
                string_values = pa.array([v.as_py().hex() if v.as_py() else None for v in stream_column])
                batch_table = batch_table.set_column(
                    batch_table.schema.get_field_index(stream_key_column), stream_key_column, string_values
                )

            t_conversion_end = time.perf_counter()
            type_conversion_time_ms = (t_conversion_end - t_conversion_start) * 1000

        # Perform inner join using PyArrow compute
        # Inner join will filter out rows where stream key doesn't match any label key
        t_join_start = time.perf_counter()
        joined_table = batch_table.join(
            label_table, keys=stream_key_column, right_keys=label_key_column, join_type='inner'
        )
        t_join_end = time.perf_counter()
        join_time_ms = (t_join_end - t_join_start) * 1000

        output_rows = joined_table.num_rows

        # Convert back to RecordBatch
        if joined_table.num_rows == 0:
            # Empty result - return empty batch with joined schema
            # Need to create empty arrays for each column
            empty_data = {field.name: pa.array([], type=field.type) for field in joined_table.schema}
            result = pa.RecordBatch.from_pydict(empty_data, schema=joined_table.schema)
        else:
            # Return as a single batch (assuming batch sizes are manageable)
            result = joined_table.to_batches()[0]

        # Log timing to stderr
        t_end = time.perf_counter()
        total_time_ms = (t_end - t_start) * 1000

        # Build timing message
        if type_conversion_time_ms > 0:
            timing_msg = (
                f'⏱️  Label join: {input_rows} → {output_rows} rows in {total_time_ms:.2f}ms '
                f'(type_conv={type_conversion_time_ms:.2f}ms, join={join_time_ms:.2f}ms, '
                f'{output_rows / total_time_ms * 1000:.0f} rows/sec) '
                f'[label={label_name}, retained={output_rows / input_rows * 100:.1f}%]\n'
            )
        else:
            timing_msg = (
                f'⏱️  Label join: {input_rows} → {output_rows} rows in {total_time_ms:.2f}ms '
                f'(join={join_time_ms:.2f}ms, {output_rows / total_time_ms * 1000:.0f} rows/sec) '
                f'[label={label_name}, retained={output_rows / input_rows * 100:.1f}%]\n'
            )

        sys.stderr.write(timing_msg)
        sys.stderr.flush()

        return result

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
