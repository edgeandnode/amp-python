from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
from psycopg2.pool import ThreadedConnectionPool

from ...streaming.state import BatchIdentifier
from ...streaming.types import BlockRange
from ..base import DataLoader, LoadMode
from ._postgres_helpers import has_binary_columns, prepare_csv_data, prepare_insert_data


@dataclass
class PostgreSQLConfig:
    """Configuration for PostgreSQL loader"""

    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    max_connections: int = 10
    batch_size: int = 10000
    connection_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.connection_params is None:
            self.connection_params = {}


class PostgreSQLLoader(DataLoader[PostgreSQLConfig]):
    """PostgreSQL data loader with zero-copy operations and connection pooling."""

    # Declare loader capabilities
    SUPPORTED_MODES = {LoadMode.APPEND, LoadMode.OVERWRITE}
    REQUIRES_SCHEMA_MATCH = False
    SUPPORTS_TRANSACTIONS = True

    def __init__(self, config: Dict[str, Any], label_manager=None) -> None:
        super().__init__(config, label_manager=label_manager)
        self.pool: Optional[ThreadedConnectionPool] = None

    def _get_required_config_fields(self) -> list[str]:
        """Return required configuration fields"""
        return ['host', 'database', 'user', 'password']

    def connect(self) -> None:
        """Establish connection pool to PostgreSQL"""
        try:
            # Create connection pool for efficient connection reuse
            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                **self.config.connection_params,
            )

            # Test connection
            with self.pool.getconn() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute('SELECT version();')
                        version = cur.fetchone()
                        self.logger.info(f'Connected to PostgreSQL: {version[0][:50]}...')

                        cur.execute('SELECT current_database()')
                        database = cur.fetchone()
                        self.logger.info(f'Connected to database: {database[0]}')  # Fixed: access first element

                        cur.execute("""SELECT
                        table_schema || '.' || table_name
                        FROM
                        information_schema.tables
                        WHERE
                        table_type = 'BASE TABLE'
                        AND
                        table_schema NOT IN ('pg_catalog', 'information_schema')
                        LIMIT 10""")
                        tables = cur.fetchall()
                        self.logger.info(f'Found {len(tables)} user tables')
                finally:
                    self.pool.putconn(conn)

            # State store is initialized in base class with in-memory storage by default
            # Future: Add database-backed persistent state store for PostgreSQL
            # For now, in-memory state provides idempotency and resumability within a session
            self._is_connected = True

        except Exception as e:
            self.logger.error(f'Failed to connect to PostgreSQL: {str(e)}')
            raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection pool"""
        if self.pool:
            self.pool.closeall()
            self.pool = None
        self._is_connected = False
        self.logger.info('Disconnected from PostgreSQL')

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Implementation-specific batch loading logic"""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                self._copy_arrow_data(cur, batch, table_name)
                conn.commit()
            return batch.num_rows
        finally:
            self.pool.putconn(conn)

    def load_batch_transactional(
        self,
        batch: pa.RecordBatch,
        table_name: str,
        connection_name: str,
        ranges: List[BlockRange],
        ranges_complete: bool = False,
    ) -> int:
        """
        Load a batch with transactional exactly-once semantics using in-memory state.

        This method uses the in-memory state store for duplicate detection,
        then loads data. The state check happens outside the transaction for simplicity,
        as the in-memory store provides session-level idempotency.

        For persistent transactional semantics across restarts, a future enhancement
        would be to implement a PostgreSQL-backed StreamStateStore.

        Args:
            batch: PyArrow RecordBatch to load
            table_name: Target table name
            connection_name: Connection identifier for tracking
            ranges: Block ranges covered by this batch
            ranges_complete: True when this RecordBatch completes a microbatch (streaming only)

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
        # Multiple RecordBatches can share the same microbatch ID (BlockRange)
        if batch_ids and ranges_complete and self.state_store.is_processed(connection_name, table_name, batch_ids):
            self.logger.info(
                f'Batch already processed (ranges: {[f"{r.network}:{r.start}-{r.end}" for r in ranges]}), '
                f'skipping (state check)'
            )
            return 0

        # Load data (always load, even if part of larger microbatch)
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                self._copy_arrow_data(cur, batch, table_name)
                conn.commit()

            # Mark as processed ONLY when microbatch is complete
            # This ensures we don't skip subsequent RecordBatches within the same microbatch
            if batch_ids and ranges_complete:
                self.state_store.mark_processed(connection_name, table_name, batch_ids)
                self.logger.debug(f'Marked microbatch as processed: {len(batch_ids)} batch IDs')

            self.logger.debug(
                f'Batch load committed: {batch.num_rows} rows, '
                f'ranges: {[f"{r.network}:{r.start}-{r.end}" for r in ranges]}'
            )
            return batch.num_rows

        except Exception as e:
            self.logger.error(f'Batch load failed: {e}')
            raise
        finally:
            self.pool.putconn(conn)

    def _clear_table(self, table_name: str) -> None:
        """Clear table for overwrite mode"""
        # Check if table exists first
        if not self.table_exists(table_name):
            return  # Nothing to clear if table doesn't exist

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE {table_name}')
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def _copy_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str) -> None:
        """Copy Arrow data to PostgreSQL using optimal method based on data types."""
        # Use INSERT for data with binary columns OR metadata columns
        # Check for both old and new metadata column names for backward compatibility
        has_metadata = (
            '_meta_block_ranges' in data.schema.names
            or '_amp_batch_id' in data.schema.names
            or '_amp_block_ranges' in data.schema.names
        )
        if has_binary_columns(data.schema) or has_metadata:
            self._insert_arrow_data(cursor, data, table_name)
        else:
            self._csv_copy_arrow_data(cursor, data, table_name)

    def _csv_copy_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str) -> None:
        """Use CSV COPY for non-binary data."""
        csv_buffer, column_names = prepare_csv_data(data)

        try:
            cursor.copy_from(csv_buffer, table_name, columns=column_names, sep='\t', null='\\N')
        except Exception as e:
            if 'does not exist' in str(e):
                raise RuntimeError(
                    f"Table '{table_name}' does not exist. Set create_table=True to auto-create. error: {e}"
                ) from e
            elif 'permission denied' in str(e).lower():
                raise RuntimeError(f"Permission denied writing to table '{table_name}'. Check user permissions.") from e
            else:
                raise RuntimeError(f'COPY operation failed: {str(e)}') from e

    def _insert_arrow_data(self, cursor: Any, data: Union[pa.RecordBatch, pa.Table], table_name: str) -> None:
        """Use INSERT statements for data with binary columns."""
        insert_sql_template, rows = prepare_insert_data(data)
        insert_sql = f'INSERT INTO {table_name} {insert_sql_template}'

        try:
            cursor.executemany(insert_sql, rows)
        except Exception as e:
            raise RuntimeError(f'INSERT operation failed: {str(e)}') from e

    def _create_table_from_schema(self, schema: pa.Schema, table_name: str) -> None:
        """Create PostgreSQL table from Arrow schema with comprehensive type mapping"""

        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                # Check if table already exists to avoid unnecessary work
                cursor.execute(
                    """
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s AND table_schema = 'public'
                """,
                    (table_name,),
                )

                if cursor.fetchone():
                    self.logger.debug(f"Table '{table_name}' already exists, skipping creation")
                    return

                # Comprehensive Arrow to PostgreSQL type mapping
                type_mapping = {
                    # Integer types
                    pa.int8(): 'SMALLINT',
                    pa.int16(): 'SMALLINT',
                    pa.int32(): 'INTEGER',
                    pa.int64(): 'BIGINT',
                    pa.uint8(): 'SMALLINT',
                    pa.uint16(): 'INTEGER',
                    pa.uint32(): 'BIGINT',
                    # Use NUMERIC for uint64 to handle values > 2^63-1 (common in blockchain data)
                    pa.uint64(): 'NUMERIC(20,0)',
                    # Floating point types
                    pa.float32(): 'REAL',
                    pa.float64(): 'DOUBLE PRECISION',
                    pa.float16(): 'REAL',
                    # String types - use TEXT for blockchain data which can be large
                    pa.string(): 'TEXT',
                    pa.large_string(): 'TEXT',
                    pa.utf8(): 'TEXT',
                    # Binary types - use BYTEA for efficient storage
                    pa.binary(): 'BYTEA',
                    pa.large_binary(): 'BYTEA',
                    # Boolean type
                    pa.bool_(): 'BOOLEAN',
                    # Date and time types
                    pa.date32(): 'DATE',
                    pa.date64(): 'DATE',
                    pa.time32('s'): 'TIME',
                    pa.time32('ms'): 'TIME',
                    pa.time64('us'): 'TIME',
                    pa.time64('ns'): 'TIME',
                }

                # Build CREATE TABLE statement
                columns = []

                for field in schema:
                    # Skip generic metadata columns - we'll use _meta_block_ranges instead
                    if field.name in ('_meta_range_start', '_meta_range_end'):
                        continue
                    # Special handling for JSONB metadata column
                    elif field.name == '_meta_block_ranges':
                        pg_type = 'JSONB'
                    # Handle complex types
                    elif pa.types.is_timestamp(field.type):
                        # Handle timezone-aware timestamps
                        if field.type.tz is not None:
                            pg_type = 'TIMESTAMPTZ'
                        else:
                            pg_type = 'TIMESTAMP'
                    elif pa.types.is_date(field.type):
                        pg_type = 'DATE'
                    elif pa.types.is_time(field.type):
                        pg_type = 'TIME'
                    elif pa.types.is_decimal(field.type):
                        # Extract precision and scale from decimal type
                        decimal_type = field.type
                        pg_type = f'NUMERIC({decimal_type.precision},{decimal_type.scale})'
                    elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                        # Use TEXT for list types (JSON-like data)
                        pg_type = 'TEXT'
                    elif pa.types.is_struct(field.type):
                        # Use TEXT for struct types (JSON-like data)
                        pg_type = 'TEXT'
                    elif pa.types.is_binary(field.type):
                        # Binary data - use BYTEA for efficient storage
                        pg_type = 'BYTEA'
                    elif pa.types.is_large_binary(field.type):
                        # Large binary data - use BYTEA for efficient storage
                        pg_type = 'BYTEA'
                    elif pa.types.is_fixed_size_binary(field.type):
                        # Fixed size binary data - use BYTEA for efficient storage
                        pg_type = 'BYTEA'
                    else:
                        # Use mapping or default to TEXT for unknown types
                        pg_type = type_mapping.get(field.type, 'TEXT')

                    # Handle nullability
                    nullable = '' if field.nullable else ' NOT NULL'

                    # Quote column name for safety (important for blockchain field names)
                    columns.append(f'"{field.name}" {pg_type}{nullable}')

                # Always add metadata columns for streaming/reorg support
                # This supports hybrid streaming (parallel catch-up → continuous streaming)
                # where initial batches don't have metadata but later ones do
                schema_field_names = [field.name for field in schema]

                # Add compact batch_id column (primary metadata for fast reorg invalidation)
                if '_amp_batch_id' not in schema_field_names:
                    # Use TEXT for compact batch identifiers (16 hex chars per batch)
                    # This column is optional and can be NULL for non-streaming loads
                    columns.append('"_amp_batch_id" TEXT')

                # Optionally add full metadata for debugging (if coming from base loader with store_full_metadata=True)
                if '_amp_block_ranges' not in schema_field_names and '_amp_block_ranges' in [f.name for f in schema]:
                    columns.append('"_amp_block_ranges" JSONB')

                # Create the table - Fixed: use proper identifier quoting
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns)}
                )
                """

                self.logger.info(f"Creating table '{table_name}' with {len(columns)} columns")
                cursor.execute(create_sql)
                conn.commit()

                # Create index on batch_id for fast reorg queries
                if '_amp_batch_id' not in schema_field_names:
                    try:
                        index_sql = (
                            f'CREATE INDEX IF NOT EXISTS idx_{table_name}_amp_batch_id ON {table_name}("_amp_batch_id")'
                        )
                        cursor.execute(index_sql)
                        conn.commit()
                        self.logger.debug(f"Created index on _amp_batch_id for table '{table_name}'")
                    except Exception as e:
                        self.logger.warning(f'Could not create index on _amp_batch_id: {e}')

                self.logger.debug(f"Successfully created table '{table_name}'")
        except Exception as e:
            raise RuntimeError(f"Failed to create table '{table_name}': {str(e)}") from e
        finally:
            self.pool.putconn(conn)

    def get_table_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get the schema of an existing PostgreSQL table"""
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Query PostgreSQL information schema
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """,
                        (table_name,),
                    )

                    columns = cur.fetchall()
                    if not columns:
                        return None

                    # Convert PostgreSQL types back to Arrow types
                    fields = []
                    for col_name, data_type, is_nullable in columns:
                        arrow_type = self._pg_type_to_arrow(data_type)
                        nullable = is_nullable.upper() == 'YES'
                        fields.append(pa.field(col_name, arrow_type, nullable))

                    return pa.schema(fields)

            finally:
                self.pool.putconn(conn)

        except Exception as e:
            self.logger.error(f"Failed to get schema for table '{table_name}': {str(e)}")
            return None

    def _pg_type_to_arrow(self, pg_type: str) -> pa.DataType:
        """Convert PostgreSQL type to Arrow type"""
        pg_type = pg_type.upper()

        # Type mapping from PostgreSQL to Arrow
        type_mapping = {
            'SMALLINT': pa.int16(),
            'INTEGER': pa.int32(),
            'BIGINT': pa.int64(),
            'REAL': pa.float32(),
            'DOUBLE PRECISION': pa.float64(),
            'TEXT': pa.string(),
            'VARCHAR': pa.string(),
            'CHAR': pa.string(),
            'BYTEA': pa.binary(),
            'BOOLEAN': pa.bool_(),
            'DATE': pa.date32(),
            'TIME': pa.time64('us'),
            'TIMESTAMP': pa.timestamp('us'),
            'TIMESTAMPTZ': pa.timestamp('us', tz='UTC'),
            'JSONB': pa.string(),
            'JSON': pa.string(),
        }

        # Handle NUMERIC types with precision/scale
        if pg_type.startswith('NUMERIC'):
            return pa.decimal128(18, 6)  # Default precision/scale

        return type_mapping.get(pg_type, pa.string())  # Default to string

    def _handle_reorg(self, invalidation_ranges: List[BlockRange], table_name: str, connection_name: str) -> None:
        """
        Handle blockchain reorganization by deleting affected rows using batch IDs.

        This method uses the state_store to find affected batch IDs, then performs
        fast indexed deletion using those IDs. This is much faster than JSON queries.

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
            # Get batch IDs that need to be deleted from state store
            affected_batch_ids = self.state_store.invalidate_from_block(
                connection_name, table_name, range_obj.network, range_obj.start
            )
            all_affected_batch_ids.extend(affected_batch_ids)

        if not all_affected_batch_ids:
            self.logger.info(f'No batches to delete for reorg in {table_name}')
            return

        # Delete rows using batch IDs (fast with index on _amp_batch_id)
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # Build list of unique IDs to delete
                unique_batch_ids = list(set(bid.unique_id for bid in all_affected_batch_ids))

                # Delete in chunks to avoid query size limits
                chunk_size = 1000
                total_deleted = 0

                for i in range(0, len(unique_batch_ids), chunk_size):
                    chunk = unique_batch_ids[i : i + chunk_size]

                    # Use LIKE with ANY for multi-batch deletion (handles "|"-separated IDs)
                    # This matches rows where _amp_batch_id contains any of the affected IDs
                    delete_sql = f"""
                        DELETE FROM {table_name}
                        WHERE "_amp_batch_id" LIKE ANY(%s)
                    """
                    # Create patterns like '%batch_id%' to match multi-network batches
                    patterns = [f'%{bid}%' for bid in chunk]
                    cur.execute(delete_sql, (patterns,))

                    deleted_count = cur.rowcount
                    total_deleted += deleted_count
                    self.logger.debug(f'Deleted {deleted_count} rows for reorg (chunk {i // chunk_size + 1})')

                conn.commit()

                self.logger.info(
                    f'Deleted {total_deleted} rows for reorg in {table_name} ({len(all_affected_batch_ids)} batch IDs)'
                )

        except Exception as e:
            self.logger.error(f"Failed to handle blockchain reorg for table '{table_name}': {str(e)}")
            raise
        finally:
            self.pool.putconn(conn)
