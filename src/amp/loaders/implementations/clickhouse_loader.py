"""
ClickHouse data loader for OLAP use cases.

This loader provides high-performance data loading to ClickHouse databases
using the clickhouse-connect library with native PyArrow support.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyarrow as pa

from ...streaming.state import BatchIdentifier
from ...streaming.types import BlockRange
from ..base import DataLoader, LoadMode


@dataclass
class ClickHouseConfig:
    """Configuration for ClickHouse loader"""

    # Connection settings
    host: str = 'localhost'
    port: int = 8123  # HTTP interface port
    database: str = 'default'
    username: str = 'default'
    password: str = ''

    # Connection pool settings
    send_receive_timeout: int = 300  # 5 minutes
    query_limit: int = 0  # No limit
    compress: bool = True  # Enable compression for better performance

    # Performance settings
    batch_size: int = 100000  # ClickHouse handles large batches well
    insert_block_size: int = 1048576  # 1M rows per block

    # Additional connection params
    connection_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}


class ClickHouseLoader(DataLoader[ClickHouseConfig]):
    """
    ClickHouse data loader with native PyArrow support for OLAP use cases.

    Features:
    - Native PyArrow integration via insert_arrow() for zero-copy operations
    - Batch insert support with configurable block sizes
    - Automatic schema mapping from Arrow to ClickHouse types
    - Comprehensive error handling and retry logic
    - Support for streaming/reorg operations

    ClickHouse is optimized for:
    - Analytical (OLAP) workloads
    - Large batch inserts
    - Columnar data compression
    - Fast aggregations and scans

    Example:
        config = {
            'host': 'localhost',
            'port': 8123,
            'database': 'analytics',
            'username': 'default',
            'password': ''
        }
        loader = ClickHouseLoader(config)
        loader.connect()
        result = loader.load_table(arrow_table, 'events')
    """

    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = False  # ClickHouse uses eventual consistency

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        super().__init__(config, label_manager=label_manager)
        self._client = None

    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['host']  # Only host is truly required, others have defaults

    def connect(self) -> None:
        """Establish connection to ClickHouse"""
        try:
            import clickhouse_connect

            # Create client with connection settings
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.username,
                password=self.config.password,
                send_receive_timeout=self.config.send_receive_timeout,
                query_limit=self.config.query_limit,
                compress=self.config.compress,
                **self.config.connection_params,
            )

            # Test connection and get server info
            version = self._client.server_version
            self.logger.info(f'Connected to ClickHouse {version} at {self.config.host}:{self.config.port}')
            self.logger.info(f'Database: {self.config.database}')

            # Get list of tables in the database
            tables = self._client.query('SELECT name FROM system.tables WHERE database = currentDatabase() LIMIT 10')
            self.logger.info(f'Found {len(tables.result_rows)} tables in database')

            self._is_connected = True

        except ImportError as err:
            raise ImportError(
                'clickhouse-connect is required for ClickHouse loader. Install it with: pip install clickhouse-connect'
            ) from err
        except Exception as e:
            self.logger.error(f'Failed to connect to ClickHouse: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Close ClickHouse connection"""
        if self._client:
            self._client.close()
            self._client = None
        self._is_connected = False
        self.logger.info('Disconnected from ClickHouse')

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Implementation-specific batch loading logic using native Arrow insertion"""
        # Convert RecordBatch to Table for insert_arrow
        table = pa.Table.from_batches([batch])

        # Use native Arrow insertion for zero-copy performance
        self._client.insert_arrow(
            table_name,
            table,
        )

        return batch.num_rows

    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create ClickHouse table from Arrow schema with comprehensive type mapping"""

        # Check if table already exists
        if self.table_exists(table_name):
            self.logger.debug(f"Table '{table_name}' already exists, skipping creation")
            return

        # Build CREATE TABLE statement
        columns = []

        for field in schema:
            # Skip internal metadata columns
            if field.name in ('_meta_range_start', '_meta_range_end'):
                continue

            # Get ClickHouse type from Arrow type
            ch_type = self._arrow_to_clickhouse_type(field.type)

            # Handle nullability - ClickHouse uses Nullable() wrapper
            if field.nullable and not ch_type.startswith('Nullable'):
                ch_type = f'Nullable({ch_type})'

            columns.append(f'`{field.name}` {ch_type}')

        # Add metadata columns for streaming/reorg support
        schema_field_names = [f.name for f in schema]

        if '_amp_batch_id' not in schema_field_names:
            columns.append('`_amp_batch_id` Nullable(String)')

        if '_amp_block_ranges' not in schema_field_names and self.store_full_metadata:
            columns.append('`_amp_block_ranges` Nullable(String)')

        # Create MergeTree table (most common ClickHouse table engine)
        # Uses tuple() for ORDER BY to allow any query patterns
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """

        self.logger.info(f"Creating table '{table_name}' with {len(columns)} columns")
        self._client.command(create_sql)
        self.logger.debug(f"Successfully created table '{table_name}'")

        # Create index on batch_id for fast reorg queries
        if '_amp_batch_id' not in schema_field_names:
            try:
                index_sql = f"""
                ALTER TABLE {table_name}
                ADD INDEX idx_amp_batch_id (`_amp_batch_id`) TYPE bloom_filter GRANULARITY 1
                """
                self._client.command(index_sql)
                self.logger.debug(f"Created bloom filter index on _amp_batch_id for table '{table_name}'")
            except Exception as e:
                self.logger.warning(f'Could not create index on _amp_batch_id: {e}')

    def _arrow_to_clickhouse_type(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow data type to ClickHouse type"""

        # Direct type mappings
        type_mapping = {
            pa.int8(): 'Int8',
            pa.int16(): 'Int16',
            pa.int32(): 'Int32',
            pa.int64(): 'Int64',
            pa.uint8(): 'UInt8',
            pa.uint16(): 'UInt16',
            pa.uint32(): 'UInt32',
            pa.uint64(): 'UInt64',
            pa.float16(): 'Float32',
            pa.float32(): 'Float32',
            pa.float64(): 'Float64',
            pa.bool_(): 'Bool',
            pa.string(): 'String',
            pa.large_string(): 'String',
            pa.utf8(): 'String',
            pa.binary(): 'String',  # Store binary as hex-encoded String
            pa.large_binary(): 'String',
            pa.date32(): 'Date',
            pa.date64(): 'Date',
        }

        # Check direct mapping first
        if arrow_type in type_mapping:
            return type_mapping[arrow_type]

        # Handle complex types
        if pa.types.is_timestamp(arrow_type):
            if arrow_type.tz is not None:
                return f"DateTime64(6, '{arrow_type.tz}')"
            return 'DateTime64(6)'

        if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
            return 'String'  # ClickHouse doesn't have a native Time type

        if pa.types.is_decimal(arrow_type):
            return f'Decimal({arrow_type.precision}, {arrow_type.scale})'

        if pa.types.is_fixed_size_binary(arrow_type):
            # Store as FixedString for efficiency
            return f'FixedString({arrow_type.byte_width})'

        if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            inner_type = self._arrow_to_clickhouse_type(arrow_type.value_type)
            return f'Array({inner_type})'

        if pa.types.is_struct(arrow_type):
            # Convert struct to JSON String
            return 'String'

        if pa.types.is_map(arrow_type):
            key_type = self._arrow_to_clickhouse_type(arrow_type.key_type)
            value_type = self._arrow_to_clickhouse_type(arrow_type.item_type)
            return f'Map({key_type}, {value_type})'

        # Default to String for unknown types
        self.logger.warning(f'Unknown Arrow type {arrow_type}, defaulting to String')
        return 'String'

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in ClickHouse"""
        try:
            result = self._client.query(
                f"SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'"
            )
            return len(result.result_rows) > 0
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get the schema of an existing ClickHouse table"""
        try:
            result = self._client.query(f'DESCRIBE TABLE {table_name}')

            fields = []
            for row in result.result_rows:
                col_name = row[0]
                col_type = row[1]

                arrow_type = self._clickhouse_to_arrow_type(col_type)
                nullable = 'Nullable' in col_type
                fields.append(pa.field(col_name, arrow_type, nullable))

            return pa.schema(fields)
        except Exception as e:
            self.logger.error(f"Failed to get schema for table '{table_name}': {e}")
            return None

    def _clickhouse_to_arrow_type(self, ch_type: str) -> pa.DataType:
        """Convert ClickHouse type to Arrow type"""
        # Strip Nullable wrapper
        if ch_type.startswith('Nullable('):
            ch_type = ch_type[9:-1]

        # Direct mappings
        type_mapping = {
            'Int8': pa.int8(),
            'Int16': pa.int16(),
            'Int32': pa.int32(),
            'Int64': pa.int64(),
            'UInt8': pa.uint8(),
            'UInt16': pa.uint16(),
            'UInt32': pa.uint32(),
            'UInt64': pa.uint64(),
            'Float32': pa.float32(),
            'Float64': pa.float64(),
            'Bool': pa.bool_(),
            'String': pa.string(),
            'Date': pa.date32(),
            'DateTime': pa.timestamp('s'),
        }

        if ch_type in type_mapping:
            return type_mapping[ch_type]

        # Handle DateTime64
        if ch_type.startswith('DateTime64'):
            return pa.timestamp('us')

        # Handle Decimal
        if ch_type.startswith('Decimal'):
            # Parse Decimal(P, S)
            params = ch_type[8:-1].split(',')
            precision = int(params[0].strip())
            scale = int(params[1].strip()) if len(params) > 1 else 0
            return pa.decimal128(precision, scale)

        # Handle FixedString
        if ch_type.startswith('FixedString'):
            length = int(ch_type[12:-1])
            return pa.binary(length)

        # Handle Array
        if ch_type.startswith('Array'):
            inner_type_str = ch_type[6:-1]
            inner_type = self._clickhouse_to_arrow_type(inner_type_str)
            return pa.list_(inner_type)

        # Default to string
        return pa.string()

    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table"""
        try:
            result = self._client.query(f'SELECT count() FROM {table_name}')
            return result.result_rows[0][0]
        except Exception as e:
            self.logger.error(f"Failed to get row count for table '{table_name}': {e}")
            return 0

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str, connection_name: str) -> None:
        """
        Handle blockchain reorganization by deleting affected rows using batch IDs.

        This method uses the state_store to find affected batch IDs, then performs
        deletion using those IDs.

        Args:
            invalidation_ranges: List of block ranges to invalidate (reorg points)
            table_name: The table containing the data to invalidate
            connection_name: Connection identifier for state lookup
        """
        if not invalidation_ranges:
            return

        # Collect all affected batch IDs from state store
        all_affected_batch_ids = []
        for range_obj in invalidation_ranges:
            affected_batch_ids = self.state_store.invalidate_from_block(
                connection_name, table_name, range_obj.network, range_obj.start
            )
            all_affected_batch_ids.extend(affected_batch_ids)

        if not all_affected_batch_ids:
            self.logger.info(f'No batches to delete for reorg in {table_name}')
            return

        # Build list of unique IDs to delete
        unique_batch_ids = list(set(bid.unique_id for bid in all_affected_batch_ids))

        # Delete in chunks to avoid query size limits
        chunk_size = 1000

        for i in range(0, len(unique_batch_ids), chunk_size):
            chunk = unique_batch_ids[i : i + chunk_size]

            # Optimize: use exact IN match for single-network IDs, LIKE for multi-network
            single_ids = [bid for bid in chunk if '|' not in bid]
            multi_ids = [bid for bid in chunk if '|' in bid]

            conditions = []
            if single_ids:
                # Exact match is faster with bloom filter index
                quoted_ids = ', '.join([f"'{bid}'" for bid in single_ids])
                conditions.append(f'`_amp_batch_id` IN ({quoted_ids})')
            if multi_ids:
                # LIKE needed for multi-network batch IDs containing "|"
                conditions.extend([f"`_amp_batch_id` LIKE '%{bid}%'" for bid in multi_ids])

            delete_sql = f'ALTER TABLE {table_name} DELETE WHERE {" OR ".join(conditions)} SETTINGS mutations_sync = 1'

            try:
                self._client.command(delete_sql)
                self.logger.debug(f'Deleted rows for reorg (chunk {i // chunk_size + 1})')
            except Exception as e:
                self.logger.error(f'Failed to delete reorg chunk: {e}')
                raise

        self.logger.info(f'Initiated deletion for reorg in {table_name} ({len(all_affected_batch_ids)} batch IDs)')

    def load_batch_transactional(
        self,
        batch: pa.RecordBatch,
        table_name: str,
        connection_name: str,
        ranges: List[BlockRange],
        ranges_complete: bool = False,
    ) -> int:
        """
        Load a batch with exactly-once semantics using in-memory state.

        Note: ClickHouse doesn't support true transactions, so we use state-based
        duplicate detection for idempotency.

        Args:
            batch: PyArrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier for tracking
            ranges: Block ranges covered by this batch
            ranges_complete: True when this RecordBatch completes a microbatch

        Returns:
            Number of rows loaded (0 if duplicate)
        """
        if not self.state_enabled:
            raise ValueError('Transactional loading requires state management to be enabled')

        # Convert ranges to batch identifiers
        try:
            batch_ids = [BatchIdentifier.from_block_range(br) for br in ranges]
        except ValueError as e:
            self.logger.warning(f'Cannot create batch identifiers: {e}. Loading without duplicate check.')
            batch_ids = []

        # Check if already processed ONLY when microbatch is complete
        if batch_ids and ranges_complete and self.state_store.is_processed(connection_name, table_name, batch_ids):
            self.logger.info(
                f'Batch already processed (ranges: {[f"{r.network}:{r.start}-{r.end}" for r in ranges]}), '
                f'skipping (state check)'
            )
            return 0

        # Load data
        rows_loaded = self._load_batch_impl(batch, table_name)

        # Mark as processed ONLY when microbatch is complete
        if batch_ids and ranges_complete:
            self.state_store.mark_processed(connection_name, table_name, batch_ids)
            self.logger.debug(f'Marked microbatch as processed: {len(batch_ids)} batch IDs')

        self.logger.debug(
            f'Batch load completed: {rows_loaded} rows, ranges: {[f"{r.network}:{r.start}-{r.end}" for r in ranges]}'
        )
        return rows_loaded

    def _get_loader_batch_metadata(self, batch: pa.RecordBatch, duration: float, **kwargs) -> Dict[str, Any]:
        """Get ClickHouse-specific metadata for batch operation"""
        return {
            'database': self.config.database,
            'engine': 'MergeTree',
        }

    def _get_loader_table_metadata(
        self, table: pa.Table, duration: float, batch_count: int, **kwargs
    ) -> Dict[str, Any]:
        """Get ClickHouse-specific metadata for table operation"""
        return {
            'database': self.config.database,
            'engine': 'MergeTree',
        }

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on ClickHouse connection"""
        try:
            if not self._client:
                return {'healthy': False, 'error': 'Not connected'}

            # Test basic connectivity
            self._client.query('SELECT 1')

            # Get server info
            version = self._client.server_version

            return {
                'healthy': True,
                'server_version': version,
                'database': self.config.database,
                'host': self.config.host,
                'port': self.config.port,
            }

        except Exception as e:
            return {'healthy': False, 'error': str(e)}

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a ClickHouse table including row count and size."""
        try:
            # Query system.parts for table statistics
            result = self._client.query(f"""
                SELECT
                    count() as part_count,
                    sum(rows) as row_count,
                    sum(bytes) as size_bytes,
                    any(engine) as engine
                FROM system.parts
                WHERE table = '{table_name}'
                  AND database = currentDatabase()
                  AND active
            """)

            if result.result_rows and result.result_rows[0][1] is not None:
                row = result.result_rows[0]
                return {
                    'table_name': table_name,
                    'part_count': row[0],
                    'row_count': row[1],
                    'size_bytes': row[2],
                    'engine': row[3],
                }

            # Table might exist but be empty - check system.tables
            exists_result = self._client.query(
                f"SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'"
            )
            if exists_result.result_rows:
                return {
                    'table_name': table_name,
                    'part_count': 0,
                    'row_count': 0,
                    'size_bytes': 0,
                    'engine': exists_result.result_rows[0][0],
                }

            return None
        except Exception as e:
            self.logger.error(f"Failed to get table info for '{table_name}': {e}")
            return None
