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

        # Get the actual config type from the generic parameter
        for base in self.__orig_bases__:
            if hasattr(base, '__args__') and base.__args__:
                config_type = base.__args__[0]
                # Check if it's a real type (not TypeVar)
                if hasattr(config_type, '__name__'):
                    try:
                        return config_type(**config)
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
        """Load a single Arrow RecordBatch with common error handling and timing"""
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
                ops_per_second=round(rows_loaded / duration, 2),
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
            return LoadResult(
                rows_loaded=rows_loaded,
                duration=time.time() - start_time,
                ops_per_second=round(rows_loaded / duration, 2),
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

        This method handles streaming data that includes reorganization events.
        When a reorg is detected, it calls _handle_reorg to let the loader
        implementation handle the invalidation appropriately.

        Args:
            stream_iterator: Iterator yielding ResponseBatchWithReorg objects
            table_name: Target table name
            **kwargs: Additional options passed to load_batch

        Yields:
            LoadResult for each batch or reorg event
        """
        if not self._is_connected:
            self.connect()

        rows_loaded = 0
        start_time = time.time()
        batch_count = 0
        reorg_count = 0

        try:
            for response in stream_iterator:
                if response.is_reorg:
                    # Handle reorganization
                    reorg_count += 1
                    duration = time.time() - start_time

                    try:
                        # Let the loader implementation handle the reorg
                        self._handle_reorg(response.invalidation_ranges, table_name)

                        # Yield a reorg result
                        yield LoadResult(
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
                else:
                    # Normal data batch
                    batch_count += 1

                    # Add metadata columns to the batch data for streaming
                    batch_data = response.data.data
                    if response.data.metadata.ranges:
                        batch_data = self._add_metadata_columns(batch_data, response.data.metadata.ranges)

                    result = self.load_batch(batch_data, table_name, **kwargs)

                    if result.success:
                        rows_loaded += result.rows_loaded

                    # Add streaming metadata
                    result.metadata['is_streaming'] = True
                    result.metadata['batch_count'] = batch_count
                    if response.data.metadata.ranges:
                        result.metadata['block_ranges'] = [
                            {'network': r.network, 'start': r.start, 'end': r.end}
                            for r in response.data.metadata.ranges
                        ]

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
