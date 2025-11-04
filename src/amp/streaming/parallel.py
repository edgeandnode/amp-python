"""
Parallel streaming implementation for high-throughput data loading.

This module implements parallel query execution using ThreadPoolExecutor.
It partitions streaming queries by block_num ranges

Key design decisions:
- Only supports streaming queries (not regular load operations)
- Block range partitioning only (block_num or _block_num columns)
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from ..loaders.types import LoadResult

if TYPE_CHECKING:
    from ..client import Client

# SQL keyword constants for query parsing
_WHERE = ' WHERE '
_ORDER_BY = ' ORDER BY '
_LIMIT = ' LIMIT '
_GROUP_BY = ' GROUP BY '
_SETTINGS = ' SETTINGS '
_STREAM_TRUE = 'STREAM = TRUE'


@dataclass
class QueryPartition:
    """Represents a partition of a query for parallel execution"""

    partition_id: int
    start_block: int
    end_block: int
    block_column: str = 'block_num'

    @property
    def metadata(self) -> Dict[str, Any]:
        """Metadata about this partition"""
        return {
            'start_block': self.start_block,
            'end_block': self.end_block,
            'block_column': self.block_column,
            'partition_size': self.end_block - self.start_block,
        }


@dataclass
class ParallelConfig:
    """Configuration for parallel streaming execution"""

    num_workers: int
    table_name: str  # Name of the table to partition (e.g., 'blocks', 'transactions')
    min_block: int = 0  # Minimum block number (defaults to 0)
    max_block: Optional[int] = None  # Maximum block number (None = auto-detect and continue streaming)
    partition_size: Optional[int] = None  # Blocks per partition (auto-calculated if not set)
    block_column: str = 'block_num'  # Column name to partition on
    stop_on_error: bool = False  # Stop all workers on first error
    reorg_buffer: int = 200  # Block overlap when transitioning to continuous streaming (for reorg detection)

    def __post_init__(self):
        if self.num_workers < 1:
            raise ValueError(f'num_workers must be >= 1, got {self.num_workers}')
        if self.max_block is not None and self.min_block >= self.max_block:
            raise ValueError(f'min_block ({self.min_block}) must be < max_block ({self.max_block})')
        if self.partition_size is not None and self.partition_size < 1:
            raise ValueError(f'partition_size must be >= 1, got {self.partition_size}')
        if not self.table_name:
            raise ValueError('table_name is required')


class BlockRangePartitionStrategy:
    """
    Strategy for partitioning streaming queries by block_num ranges.

    Injects WHERE clause filters into the user's query to partition data by
    block ranges. Handles queries with or without existing WHERE clauses.

    Example:
        User query: SELECT * FROM blocks WHERE hash IS NOT NULL
        Table: 'blocks'
        Partition: blocks 0-1000000

        Result:
        SELECT * FROM blocks WHERE hash IS NOT NULL AND (block_num >= 0 AND block_num < 1000000)

        User query: SELECT * FROM eth_firehose.blocks
        Partition: blocks 0-1000000

        Result:
        SELECT * FROM eth_firehose.blocks WHERE block_num >= 0 AND block_num < 1000000
    """

    def __init__(self, table_name: str, block_column: str = 'block_num'):
        self.table_name = table_name
        self.block_column = block_column
        self.logger = logging.getLogger(__name__)

    def create_partitions(self, config: ParallelConfig) -> List[QueryPartition]:
        """
        Create query partitions based on configuration.

        Divides the block range [min_block, max_block) into equal partitions.
        If partition_size is specified, creates as many partitions as needed.
        Otherwise, divides evenly across num_workers.

        Args:
            config: Parallel execution configuration with block range

        Returns:
            List of QueryPartition objects

        Raises:
            ValueError: If configuration is invalid
        """
        min_block = config.min_block
        max_block = config.max_block
        total_blocks = max_block - min_block

        if total_blocks <= 0:
            raise ValueError(f'Invalid block range: {min_block} to {max_block}')

        # Calculate partition size
        if config.partition_size:
            # User specified partition size
            partition_size = config.partition_size
            # Calculate actual number of partitions needed
            num_partitions = (total_blocks + partition_size - 1) // partition_size
            self.logger.info(
                f'Using partition_size={partition_size:,} blocks, '
                f'creating {num_partitions} partitions for {total_blocks:,} total blocks'
            )
        else:
            # Divide evenly across workers
            num_partitions = config.num_workers
            partition_size = (total_blocks + num_partitions - 1) // num_partitions
            self.logger.info(
                f'Auto-calculated partition_size={partition_size:,} blocks '
                f'for {num_partitions} workers, {total_blocks:,} total blocks'
            )

        # Create partitions
        partitions = []
        for i in range(num_partitions):
            start = min_block + (i * partition_size)
            end = min(start + partition_size, max_block)

            if start >= max_block:
                break

            partition = QueryPartition(
                partition_id=i, start_block=start, end_block=end, block_column=config.block_column
            )
            partitions.append(partition)

        self.logger.info(f'Created {len(partitions)} partitions from block {min_block:,} to {max_block:,}')
        return partitions

    def wrap_query_with_partition(self, user_query: str, partition: QueryPartition) -> str:
        """
        Add partition filter to user query's WHERE clause.

        Injects a block range filter into the query to partition the data.
        If the query already has a WHERE clause, appends with AND.
        If not, adds a new WHERE clause.

        Args:
            user_query: Original user query (e.g., "SELECT * FROM blocks WHERE hash IS NOT NULL")
            partition: Partition to apply

        Returns:
            Query with partition filter added
        """
        # Remove trailing semicolon if present
        user_query = user_query.strip().rstrip(';')

        # Create partition filter
        partition_filter = (
            f'{partition.block_column} >= {partition.start_block} AND {partition.block_column} < {partition.end_block}'
        )

        # Check if query already has a WHERE clause (case-insensitive)
        # Look for WHERE before any ORDER BY, LIMIT, or SETTINGS clauses
        query_upper = user_query.upper()

        # Find WHERE position
        where_pos = query_upper.find(_WHERE)

        if where_pos != -1:
            # Query has WHERE clause - append with AND
            # Need to insert before ORDER BY, LIMIT, GROUP BY, or SETTINGS if they exist
            insert_pos = where_pos + len(_WHERE)

            # Find the end of the WHERE clause (before ORDER BY, LIMIT, GROUP BY, SETTINGS)
            end_keywords = [_ORDER_BY, _LIMIT, _GROUP_BY, _SETTINGS]
            end_pos = len(user_query)

            for keyword in end_keywords:
                keyword_pos = query_upper.find(keyword, insert_pos)
                if keyword_pos != -1 and keyword_pos < end_pos:
                    end_pos = keyword_pos

            # Insert partition filter with AND
            partitioned_query = user_query[:end_pos] + f' AND ({partition_filter})' + user_query[end_pos:]
        else:
            # No WHERE clause - add one before ORDER BY, LIMIT, GROUP BY, or SETTINGS
            end_keywords = [_ORDER_BY, _LIMIT, _GROUP_BY, _SETTINGS]
            insert_pos = len(user_query)

            for keyword in end_keywords:
                keyword_pos = query_upper.find(keyword)
                if keyword_pos != -1 and keyword_pos < insert_pos:
                    insert_pos = keyword_pos

            # Insert WHERE clause with partition filter
            partitioned_query = user_query[:insert_pos] + f' WHERE {partition_filter}' + user_query[insert_pos:]

        return partitioned_query


@dataclass
class ParallelExecutionStats:
    """Statistics for parallel execution"""

    total_rows: int = 0
    total_duration: float = 0.0
    workers_completed: int = 0
    workers_failed: int = 0
    partition_results: List[Dict[str, Any]] = field(default_factory=list)


class ParallelStreamExecutor:
    """
    Executes parallel streaming queries using ThreadPoolExecutor.

    Manages:
    - Query partitioning by block ranges using CTEs
    - Worker thread pool execution
    - Result aggregation
    - Error handling
    - Progress tracking

    Note: This executor is designed for streaming queries only.
    """

    def __init__(self, client: 'Client', config: ParallelConfig):
        self.client = client
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=config.num_workers)
        self.logger = logging.getLogger(__name__)
        self._stats_lock = Lock()
        self._stats = ParallelExecutionStats()
        self.partitioner = BlockRangePartitionStrategy(config.table_name, config.block_column)

    def _detect_current_max_block(self) -> int:
        """
        Query the backend to detect the current maximum block number.

        Returns:
            Maximum block number currently available in the table

        Raises:
            RuntimeError: If query fails or returns no results
        """
        query = f'SELECT MAX({self.config.block_column}) as max_block FROM {self.config.table_name}'
        self.logger.info(f'Detecting current max block with query: {query}')

        try:
            # Execute query to get max block
            table = self.client.get_sql(query, read_all=True)

            if table.num_rows == 0:
                raise RuntimeError(f'No data found in table {self.config.table_name}')

            max_block = table.column('max_block')[0].as_py()

            if max_block is None:
                raise RuntimeError(f'No blocks found in table {self.config.table_name}')

            self.logger.info(f'Detected current max block: {max_block:,}')
            return int(max_block)

        except Exception as e:
            self.logger.error(f'Failed to detect max block: {e}')
            raise RuntimeError(f'Failed to detect current max block from {self.config.table_name}: {e}') from e

    def execute_parallel_stream(
        self, user_query: str, destination: str, connection_name: str, load_config: Optional[Dict[str, Any]] = None
    ) -> Iterator[LoadResult]:
        """
        Execute parallel streaming load with CTE-based partitioning.

        If max_block is None, auto-detects the current max block and then transitions
        to continuous streaming after the parallel catch-up phase completes.

        1. Auto-detect max_block if not specified
        2. Create partitions based on block range
        3. Wrap user query with partition CTEs
        4. Submit worker tasks to thread pool
        5. Stream results as they complete
        6. If max_block was auto-detected, transition to continuous streaming

        Args:
            user_query: User's SQL query (will be wrapped in CTE)
            destination: Target table name
            connection_name: Named connection for loader
            load_config: Additional load configuration

        Yields:
            LoadResult for each partition as it completes, then continuous streaming results
        """
        load_config = load_config or {}

        # Detect if we should continue with live streaming after parallel phase
        continue_streaming = self.config.max_block is None

        # 1. Auto-detect max_block if not specified
        if continue_streaming:
            try:
                detected_max_block = self._detect_current_max_block()
                # Create a modified config with the detected max_block for partitioning
                catchup_config = ParallelConfig(
                    num_workers=self.config.num_workers,
                    table_name=self.config.table_name,
                    min_block=self.config.min_block,
                    max_block=detected_max_block,
                    partition_size=self.config.partition_size,
                    block_column=self.config.block_column,
                    stop_on_error=self.config.stop_on_error,
                )
                self.logger.info(
                    f'Hybrid streaming mode: will catch up blocks {self.config.min_block:,} to {detected_max_block:,}, '
                    f'then continue with live streaming'
                )
            except Exception as e:
                yield LoadResult(
                    rows_loaded=0,
                    duration=0,
                    ops_per_second=0,
                    table_name=destination,
                    loader_type='parallel',
                    success=False,
                    error=f'Failed to detect max block: {e}',
                )
                return
        else:
            catchup_config = self.config
            self.logger.info(
                f'Historical load mode: loading blocks {self.config.min_block:,} to {self.config.max_block:,}'
            )

        # 2. Create partitions
        try:
            partitions = self.partitioner.create_partitions(catchup_config)
        except ValueError as e:
            self.logger.error(f'Failed to create partitions: {e}')
            yield LoadResult(
                rows_loaded=0,
                duration=0,
                ops_per_second=0,
                table_name=destination,
                loader_type='parallel',
                success=False,
                error=f'Partition creation failed: {e}',
            )
            return

        self.logger.info(
            f'Starting parallel streaming with {len(partitions)} partitions across {self.config.num_workers} workers'
        )

        # 2. Pre-flight table creation (before workers start)
        # Create table once to avoid locking complexity in parallel workers
        try:
            # Get connection info
            connection_info = self.client.connection_manager.get_connection_info(connection_name)
            loader_config = connection_info['config']
            loader_type = connection_info['loader']

            # Get sample schema by executing LIMIT 1 on original query
            # We don't need partition filtering for schema detection, just need any row
            sample_query = user_query.strip().rstrip(';')

            # Remove SETTINGS clause (especially stream = true) to avoid streaming mode
            sample_query_upper = sample_query.upper()
            settings_pos = sample_query_upper.find(_SETTINGS)
            if settings_pos != -1:
                sample_query = sample_query[:settings_pos].rstrip()
                sample_query_upper = sample_query.upper()

            # Insert LIMIT 1 before ORDER BY, GROUP BY if present
            end_keywords = [_ORDER_BY, _GROUP_BY]
            insert_pos = len(sample_query)

            for keyword in end_keywords:
                keyword_pos = sample_query_upper.find(keyword)
                if keyword_pos != -1 and keyword_pos < insert_pos:
                    insert_pos = keyword_pos

            # Insert LIMIT 1 at the correct position
            sample_query = sample_query[:insert_pos].rstrip() + ' LIMIT 1' + sample_query[insert_pos:]

            self.logger.debug(f'Fetching schema with sample query: {sample_query[:100]}...')
            sample_table = self.client.get_sql(sample_query, read_all=True)

            if sample_table.num_rows > 0:
                # Create loader instance to get effective schema and create table
                from ..loaders.registry import create_loader

                loader_instance = create_loader(loader_type, loader_config)

                try:
                    loader_instance.connect()

                    # Get schema from sample batch
                    sample_batch = sample_table.to_batches()[0]
                    effective_schema = sample_batch.schema

                    # Create table once with schema
                    if hasattr(loader_instance, '_create_table_from_schema'):
                        loader_instance._create_table_from_schema(effective_schema, destination)
                        loader_instance._created_tables.add(destination)
                        self.logger.info(f"Pre-created table '{destination}' with {len(effective_schema)} columns")
                    else:
                        self.logger.warning('Loader does not support table creation, workers will handle it')
                finally:
                    loader_instance.disconnect()
            else:
                self.logger.warning('Sample query returned no rows, skipping pre-flight table creation')

            # Update load_config to skip table creation in workers
            load_config['create_table'] = False

        except Exception as e:
            self.logger.warning(
                f'Pre-flight table creation failed: {e}. Workers will attempt table creation with locking.'
            )
            # Don't fail the entire job - let workers try to create the table

        # 3. Submit worker tasks
        futures = {}
        for partition in partitions:
            future = self.executor.submit(
                self._execute_partition, user_query, partition, destination, connection_name, load_config
            )
            futures[future] = partition

        # 4. Stream results as they complete
        try:
            for future in as_completed(futures):
                partition = futures[future]
                try:
                    result = future.result()
                    self._update_stats(result, success=True)
                    yield result
                except Exception as e:
                    self.logger.error(f'Partition {partition.partition_id} failed: {e}')
                    self._update_stats(partition, success=False)

                    if self.config.stop_on_error:
                        self.logger.error('Stopping all workers due to error')
                        self.executor.shutdown(wait=False, cancel_futures=True)
                        raise

                    # Yield error result
                    yield LoadResult(
                        rows_loaded=0,
                        duration=0,
                        ops_per_second=0,
                        table_name=destination,
                        loader_type='parallel',
                        success=False,
                        error=str(e),
                        metadata={'partition_id': partition.partition_id, 'partition_metadata': partition.metadata},
                    )
        finally:
            self.executor.shutdown(wait=True)
            self._log_final_stats()

        # 5. If in hybrid mode, transition to continuous streaming for live blocks
        if continue_streaming:
            # Start continuous streaming with a buffer for reorg overlap
            # This ensures we catch any reorgs that occurred during parallel catchup
            continuous_start_block = max(self.config.min_block, detected_max_block - self.config.reorg_buffer)

            self.logger.info(
                f'Parallel catch-up complete (loaded up to block {detected_max_block:,}). '
                f'Transitioning to continuous streaming from block {continuous_start_block:,} '
                f'(with {self.config.reorg_buffer}-block reorg buffer)...'
            )

            # Ensure query has streaming settings
            # Strip any existing SETTINGS clause first (it may have been removed by workers)
            # Then add it back for continuous streaming
            streaming_query = user_query.strip().rstrip(';')
            streaming_query_upper = streaming_query.upper()
            settings_pos = streaming_query_upper.find(' SETTINGS ')
            if settings_pos != -1:
                # Remove existing SETTINGS clause
                streaming_query = streaming_query[:settings_pos].rstrip()

            # Add block filter to start from (detected_max - buffer) to catch potential reorgs
            # Check if query already has WHERE clause
            where_pos = streaming_query_upper.find(' WHERE ')
            block_filter = f'{self.config.block_column} >= {continuous_start_block}'

            if where_pos != -1:
                # Has WHERE clause - append with AND
                # Find position after WHERE keyword
                insert_pos = where_pos + len(' WHERE ')
                streaming_query = streaming_query[:insert_pos] + f'({block_filter}) AND ' + streaming_query[insert_pos:]
            else:
                # No WHERE clause - add one before SETTINGS if present
                streaming_query += f' WHERE {block_filter}'

            # Now add streaming settings for continuous mode
            streaming_query += ' SETTINGS stream = true'

            # Start continuous streaming with reorg detection
            # No parallel_config means single-stream mode
            yield from self.client.query_and_load_streaming(
                query=streaming_query,
                destination=destination,
                connection_name=connection_name,
                with_reorg_detection=True,
                **load_config,
            )

    def _execute_partition(
        self,
        user_query: str,
        partition: QueryPartition,
        destination: str,
        connection_name: str,
        load_config: Dict[str, Any],
    ) -> LoadResult:
        """
        Execute a single partition in a worker thread.

        Each worker:
        1. Wraps user query with partition CTE
        2. Executes streaming query using client
        3. Loads results to destination
        4. Returns aggregate LoadResult

        Args:
            user_query: Original user query
            partition: Partition to execute
            destination: Target table
            connection_name: Connection name
            load_config: Load configuration

        Returns:
            Aggregated LoadResult for this partition
        """
        start_time = time.time()

        self.logger.info(
            f'Worker {partition.partition_id} starting: blocks {partition.start_block:,} to {partition.end_block:,}'
        )

        try:
            partition_query = self.partitioner.wrap_query_with_partition(user_query, partition)

            # IMPORTANT: Remove SETTINGS stream = true for historical loads
            # We want to load a specific block range and finish, not wait for new data
            partition_query_upper = partition_query.upper()
            if 'SETTINGS STREAM = TRUE' in partition_query_upper:
                idx = partition_query_upper.find('SETTINGS STREAM = TRUE')
                partition_query = partition_query[:idx].rstrip()

            # Execute query and load (NOT streaming mode - we want to load historical range and finish)
            # Use query_and_load with read_all=False to stream batches efficiently
            results_iterator = self.client.query_and_load(
                query=partition_query,
                destination=destination,
                connection_name=connection_name,
                read_all=False,  # Stream batches for memory efficiency
                **load_config,
            )

            # Aggregate results from streaming iterator
            total_rows = 0
            total_duration = 0.0
            batch_count = 0

            for result in results_iterator:
                if result.success:
                    total_rows += result.rows_loaded
                    total_duration += result.duration
                    batch_count += 1
                else:
                    self.logger.error(f'Worker {partition.partition_id} batch failed: {result.error}')
                    raise RuntimeError(f'Batch load failed: {result.error}')

            duration = time.time() - start_time

            self.logger.info(
                f'Worker {partition.partition_id} completed: '
                f'{total_rows:,} rows in {duration:.2f}s '
                f'({batch_count} batches, {total_rows / duration:.0f} rows/sec)'
            )

            # Return aggregated result
            return LoadResult(
                rows_loaded=total_rows,
                duration=duration,
                ops_per_second=total_rows / duration if duration > 0 else 0,
                table_name=destination,
                loader_type='parallel',
                success=True,
                metadata={
                    'partition_id': partition.partition_id,
                    'batches_processed': batch_count,
                    'partition_metadata': partition.metadata,
                    'worker_thread': threading.current_thread().name,
                },
            )

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f'Worker {partition.partition_id} failed after {duration:.2f}s: {e}')
            raise

    def _update_stats(self, result_or_partition, success: bool):
        """Thread-safe stats update"""
        with self._stats_lock:
            if success:
                result = result_or_partition
                self._stats.total_rows += result.rows_loaded
                self._stats.total_duration += result.duration
                self._stats.workers_completed += 1
                self._stats.partition_results.append(
                    {
                        'partition_id': result.metadata.get('partition_id'),
                        'rows': result.rows_loaded,
                        'duration': result.duration,
                        'throughput': result.ops_per_second,
                    }
                )
            else:
                self._stats.workers_failed += 1

    def _log_final_stats(self):
        """Log final execution statistics"""
        total_workers = self._stats.workers_completed + self._stats.workers_failed

        if self._stats.workers_completed > 0:
            avg_throughput = sum(p['throughput'] for p in self._stats.partition_results) / len(
                self._stats.partition_results
            )

            self.logger.info(
                f'Parallel execution complete: '
                f'{self._stats.total_rows:,} rows loaded, '
                f'{self._stats.workers_completed}/{total_workers} workers succeeded, '
                f'{self._stats.workers_failed} workers failed, '
                f'avg throughput: {avg_throughput:,.0f} rows/sec per worker'
            )
        else:
            self.logger.error(f'Parallel execution failed: all {self._stats.workers_failed} workers failed')
