# tests/integration/test_postgresql_loader.py
"""
Integration tests for PostgreSQL loader implementation.
These tests require a running PostgreSQL instance.
"""

import time
from datetime import datetime

import pyarrow as pa
import pytest

try:
    from src.amp.loaders.base import LoadMode
    from src.amp.loaders.implementations.postgresql_loader import PostgreSQLLoader
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


@pytest.fixture
def postgresql_type_test_data():
    """Create test data specifically for PostgreSQL data type testing"""
    data = {
        'id': list(range(1000)),
        'text_field': [f'text_{i}' for i in range(1000)],
        'float_field': [i * 1.23 for i in range(1000)],
        'bool_field': [i % 2 == 0 for i in range(1000)],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def cleanup_tables(postgresql_test_config):
    """Cleanup test tables after tests"""
    tables_to_clean = []

    yield tables_to_clean

    # Cleanup
    loader = PostgreSQLLoader(postgresql_test_config)
    try:
        loader.connect()
        conn = loader.pool.getconn()
        try:
            with conn.cursor() as cur:
                for table in tables_to_clean:
                    try:
                        cur.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
                        conn.commit()
                    except Exception:
                        pass
        finally:
            loader.pool.putconn(conn)
        loader.disconnect()
    except Exception:
        pass


@pytest.mark.integration
@pytest.mark.postgresql
class TestPostgreSQLLoaderIntegration:
    """Integration tests for PostgreSQL loader"""

    def test_loader_connection(self, postgresql_test_config):
        """Test basic connection to PostgreSQL"""
        loader = PostgreSQLLoader(postgresql_test_config)

        # Test connection
        loader.connect()
        assert loader._is_connected == True
        assert loader.pool is not None

        # Test disconnection
        loader.disconnect()
        assert loader._is_connected == False
        assert loader.pool is None

    def test_context_manager(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test context manager functionality"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            assert loader._is_connected == True

            result = loader.load_table(small_test_data, test_table_name)
            assert result.success == True

        # Should be disconnected after context
        assert loader._is_connected == False

    def test_basic_table_operations(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test basic table creation and data loading"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Test initial table creation
            result = loader.load_table(small_test_data, test_table_name, create_table=True)

            assert result.success == True
            assert result.rows_loaded == 5
            assert result.loader_type == 'postgresql'
            assert result.table_name == test_table_name
            assert 'columns' in result.metadata
            assert result.metadata['columns'] == 7

    def test_append_mode(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test append mode functionality"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Initial load
            result = loader.load_table(small_test_data, test_table_name, mode=LoadMode.APPEND)
            assert result.success == True
            assert result.rows_loaded == 5

            # Append additional data
            result = loader.load_table(small_test_data, test_table_name, mode=LoadMode.APPEND)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify total rows
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    count = cur.fetchone()[0]
                    assert count == 10  # 5 + 5
            finally:
                loader.pool.putconn(conn)

    def test_overwrite_mode(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test overwrite mode functionality"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Initial load
            result = loader.load_table(small_test_data, test_table_name, mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 5

            # Overwrite with different data
            new_data = small_test_data.slice(0, 3)  # First 3 rows
            result = loader.load_table(new_data, test_table_name, mode=LoadMode.OVERWRITE)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify only new data remains
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    count = cur.fetchone()[0]
                    assert count == 3
            finally:
                loader.pool.putconn(conn)

    def test_batch_loading(self, postgresql_test_config, medium_test_table, test_table_name, cleanup_tables):
        """Test batch loading functionality"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Test loading individual batches
            batches = medium_test_table.to_batches(max_chunksize=250)

            for i, batch in enumerate(batches):
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                result = loader.load_batch(batch, test_table_name, mode=mode)

                assert result.success == True
                assert result.rows_loaded == batch.num_rows
                assert result.metadata['batch_size'] == batch.num_rows

            # Verify all data was loaded
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    count = cur.fetchone()[0]
                    assert count == 10000
            finally:
                loader.pool.putconn(conn)

    def test_data_types(self, postgresql_test_config, postgresql_type_test_data, test_table_name, cleanup_tables):
        """Test various data types are handled correctly"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            result = loader.load_table(postgresql_type_test_data, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 1000

            # Verify data integrity
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Check various data types
                    cur.execute(f'SELECT id, text_field, float_field, bool_field FROM {test_table_name} WHERE id = 10')
                    row = cur.fetchone()
                    assert row[0] == 10
                    assert row[1] in ['text_10', '"text_10"']  # Handle potential CSV quoting
                    assert abs(row[2] - 12.3) < 0.01  # 10 * 1.23 = 12.3
                    assert row[3] == True
            finally:
                loader.pool.putconn(conn)

    def test_null_value_handling(self, postgresql_test_config, null_test_data, test_table_name, cleanup_tables):
        """Test comprehensive null value handling across all data types"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            result = loader.load_table(null_test_data, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 10

            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Check text field nulls (rows 3, 6, 9 have index 2, 5, 8)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE text_field IS NULL')
                    text_nulls = cur.fetchone()[0]
                    assert text_nulls == 3

                    # Check int field nulls (rows 2, 5, 8 have index 1, 4, 7)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE int_field IS NULL')
                    int_nulls = cur.fetchone()[0]
                    assert int_nulls == 3

                    # Check float field nulls (rows 3, 6, 9 have index 2, 5, 8)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE float_field IS NULL')
                    float_nulls = cur.fetchone()[0]
                    assert float_nulls == 3

                    # Check bool field nulls (rows 3, 6, 9 have index 2, 5, 8)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE bool_field IS NULL')
                    bool_nulls = cur.fetchone()[0]
                    assert bool_nulls == 3

                    # Check timestamp field nulls
                    # (rows where i % 3 == 0, which are ids 3, 6, 9, plus id 1 due to zero indexing)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE timestamp_field IS NULL')
                    timestamp_nulls = cur.fetchone()[0]
                    assert timestamp_nulls == 4

                    # Check json field nulls (rows where i % 4 == 0, which are ids 4, 8 due to zero indexing pattern)
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE json_field IS NULL')
                    json_nulls = cur.fetchone()[0]
                    assert json_nulls == 3

                    # Verify non-null values are intact
                    cur.execute(f'SELECT text_field FROM {test_table_name} WHERE id = 1')
                    text_val = cur.fetchone()[0]
                    assert text_val in ['a', '"a"']  # Handle potential CSV quoting

                    cur.execute(f'SELECT int_field FROM {test_table_name} WHERE id = 1')
                    int_val = cur.fetchone()[0]
                    assert int_val == 1

                    cur.execute(f'SELECT float_field FROM {test_table_name} WHERE id = 1')
                    float_val = cur.fetchone()[0]
                    assert abs(float_val - 1.1) < 0.01

                    cur.execute(f'SELECT bool_field FROM {test_table_name} WHERE id = 1')
                    bool_val = cur.fetchone()[0]
                    assert bool_val == True
            finally:
                loader.pool.putconn(conn)

    def test_binary_data_handling(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test binary data handling with INSERT fallback"""
        cleanup_tables.append(test_table_name)

        # Create data with binary columns
        data = {'id': [1, 2, 3], 'binary_data': [b'hello', b'world', b'test'], 'text_data': ['a', 'b', 'c']}
        table = pa.Table.from_pydict(data)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            result = loader.load_table(table, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify binary data was stored correctly
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT id, binary_data FROM {test_table_name} ORDER BY id')
                    rows = cur.fetchall()
                    assert rows[0][1].tobytes() == b'hello'
                    assert rows[1][1].tobytes() == b'world'
                    assert rows[2][1].tobytes() == b'test'
            finally:
                loader.pool.putconn(conn)

    def test_schema_retrieval(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test schema retrieval functionality"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Create table
            result = loader.load_table(small_test_data, test_table_name)
            assert result.success == True

            # Get schema
            schema = loader.get_table_schema(test_table_name)
            assert schema is not None

            # Filter out metadata columns added by PostgreSQL loader
            non_meta_fields = [field for field in schema if not field.name.startswith('_meta_')]

            assert len(non_meta_fields) == len(small_test_data.schema)

            # Verify column names match (excluding metadata columns)
            original_names = set(small_test_data.schema.names)
            retrieved_names = set(field.name for field in non_meta_fields)
            assert original_names == retrieved_names

    def test_error_handling(self, postgresql_test_config, small_test_data):
        """Test error handling scenarios"""
        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Test loading to non-existent table without create_table
            result = loader.load_table(small_test_data, 'non_existent_table', create_table=False)

            assert result.success == False
            assert result.error is not None
            assert result.rows_loaded == 0
            assert 'does not exist' in result.error

    def test_connection_pooling(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test connection pooling behavior"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Perform multiple operations to test pool reuse
            for i in range(5):
                subset = small_test_data.slice(i, 1)
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND

                result = loader.load_table(subset, test_table_name, mode=mode)
                assert result.success == True

            # Verify pool is managing connections properly
            # Note: _used is a dict in ThreadedConnectionPool, not an int
            assert len(loader.pool._used) <= loader.pool.maxconn

    def test_performance_metrics(self, postgresql_test_config, medium_test_table, test_table_name, cleanup_tables):
        """Test performance metrics in results"""
        cleanup_tables.append(test_table_name)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(medium_test_table, test_table_name)
            end_time = time.time()

            assert result.success == True
            assert result.duration > 0
            assert result.duration <= (end_time - start_time)
            assert result.rows_loaded == 10000

            # Check metadata contains performance info
            assert 'table_size_bytes' in result.metadata
            assert result.metadata['table_size_bytes'] > 0


@pytest.mark.integration
@pytest.mark.postgresql
@pytest.mark.slow
class TestPostgreSQLLoaderPerformance:
    """Performance tests for PostgreSQL loader"""

    def test_large_data_loading(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test loading large datasets"""
        cleanup_tables.append(test_table_name)

        # Create large dataset
        large_data = {
            'id': list(range(50000)),
            'value': [i * 0.123 for i in range(50000)],
            'category': [f'category_{i % 100}' for i in range(50000)],
            'description': [f'This is a longer text description for row {i}' for i in range(50000)],
            'created_at': [datetime.now() for _ in range(50000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            result = loader.load_table(large_table, test_table_name)

            assert result.success == True
            assert result.rows_loaded == 50000
            assert result.duration < 60  # Should complete within 60 seconds

            # Verify data integrity
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    count = cur.fetchone()[0]
                    assert count == 50000
            finally:
                loader.pool.putconn(conn)


@pytest.mark.integration
@pytest.mark.postgresql
class TestPostgreSQLLoaderStreaming:
    """Integration tests for PostgreSQL loader streaming functionality"""

    def test_streaming_metadata_columns(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test that streaming data creates tables with metadata columns"""
        cleanup_tables.append(test_table_name)

        # Import streaming types
        from src.amp.streaming.types import BlockRange

        # Create test data with metadata
        data = {
            'block_number': [100, 101, 102],
            'transaction_hash': ['0xabc', '0xdef', '0x123'],
            'value': [1.0, 2.0, 3.0],
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Create metadata with block ranges
        block_ranges = [BlockRange(network='ethereum', start=100, end=102)]

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Add metadata columns (simulating what load_stream_continuous does)
            batch_with_metadata = loader._add_metadata_columns(batch, block_ranges)

            # Load the batch
            result = loader.load_batch(batch_with_metadata, test_table_name, create_table=True)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify metadata columns were created in the table
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Check table schema includes metadata columns
                    cur.execute(
                        """
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """,
                        (test_table_name,),
                    )

                    columns = cur.fetchall()
                    column_names = [col[0] for col in columns]

                    # Should have original columns plus metadata columns
                    assert '_meta_block_ranges' in column_names

                    # Verify metadata column types
                    column_types = {col[0]: col[1] for col in columns}
                    assert 'jsonb' in column_types['_meta_block_ranges'].lower()

                    # Verify data was stored correctly
                    cur.execute(f'SELECT "_meta_block_ranges" FROM {test_table_name} LIMIT 1')
                    meta_row = cur.fetchone()

                    # PostgreSQL JSONB automatically parses to Python objects
                    ranges_data = meta_row[0]  # Already parsed by psycopg2
                    assert len(ranges_data) == 1
                    assert ranges_data[0]['network'] == 'ethereum'
                    assert ranges_data[0]['start'] == 100
                    assert ranges_data[0]['end'] == 102

            finally:
                loader.pool.putconn(conn)

    def test_handle_reorg_deletion(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test that _handle_reorg correctly deletes invalidated ranges"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BlockRange

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Create table and load test data with multiple block ranges
            data_batch1 = {
                'tx_hash': ['0x100', '0x101', '0x102'],
                'block_num': [100, 101, 102],
                'value': [10.0, 11.0, 12.0],
            }
            batch1 = pa.RecordBatch.from_pydict(data_batch1)
            ranges1 = [BlockRange(network='ethereum', start=100, end=102)]
            batch1_with_meta = loader._add_metadata_columns(batch1, ranges1)

            data_batch2 = {'tx_hash': ['0x200', '0x201'], 'block_num': [103, 104], 'value': [12.0, 33.0]}
            batch2 = pa.RecordBatch.from_pydict(data_batch2)
            ranges2 = [BlockRange(network='ethereum', start=103, end=104)]
            batch2_with_meta = loader._add_metadata_columns(batch2, ranges2)

            data_batch3 = {'tx_hash': ['0x200', '0x201'], 'block_num': [105, 106], 'value': [7.0, 9.0]}
            batch3 = pa.RecordBatch.from_pydict(data_batch3)
            ranges3 = [BlockRange(network='ethereum', start=103, end=104)]
            batch3_with_meta = loader._add_metadata_columns(batch3, ranges3)

            data_batch4 = {'tx_hash': ['0x200', '0x201'], 'block_num': [107, 108], 'value': [6.0, 73.0]}
            batch4 = pa.RecordBatch.from_pydict(data_batch4)
            ranges4 = [BlockRange(network='ethereum', start=103, end=104)]
            batch4_with_meta = loader._add_metadata_columns(batch4, ranges4)

            # Load all batches
            result1 = loader.load_batch(batch1_with_meta, test_table_name, create_table=True)
            result2 = loader.load_batch(batch2_with_meta, test_table_name, create_table=False)
            result3 = loader.load_batch(batch3_with_meta, test_table_name, create_table=False)
            result4 = loader.load_batch(batch4_with_meta, test_table_name, create_table=False)

            assert all([result1.success, result2.success, result3.success, result4.success])

            # Verify initial data count
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    initial_count = cur.fetchone()[0]
                    assert initial_count == 9  # 3 + 2 + 2 + 2

                    # Test reorg deletion - invalidate blocks 104-108 on ethereum
                    invalidation_ranges = [BlockRange(network='ethereum', start=104, end=108)]
                    loader._handle_reorg(invalidation_ranges, test_table_name)

                    # Should delete batch2, batch3 and batch4 leaving only the 3 rows from batch1
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    after_reorg_count = cur.fetchone()[0]
                    assert after_reorg_count == 3

            finally:
                loader.pool.putconn(conn)

    def test_reorg_with_overlapping_ranges(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test reorg deletion with overlapping block ranges"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BlockRange

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Load data with overlapping ranges that should be invalidated
            data = {'tx_hash': ['0x150', '0x175', '0x250'], 'block_num': [150, 175, 250], 'value': [15.0, 17.5, 25.0]}
            batch = pa.RecordBatch.from_pydict(data)
            ranges = [BlockRange(network='ethereum', start=150, end=175)]
            batch_with_meta = loader._add_metadata_columns(batch, ranges)

            result = loader.load_batch(batch_with_meta, test_table_name, create_table=True)
            assert result.success == True

            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Verify initial data
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 3

                    # Test partial overlap invalidation (160-180)
                    # This should invalidate our range [150,175] because they overlap
                    invalidation_ranges = [BlockRange(network='ethereum', start=160, end=180)]
                    loader._handle_reorg(invalidation_ranges, test_table_name)

                    # All data should be deleted due to overlap
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 0

            finally:
                loader.pool.putconn(conn)

    def test_reorg_preserves_different_networks(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test that reorg only affects specified network"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BlockRange

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Load data from multiple networks with same block ranges
            data_eth = {'tx_hash': ['0x100_eth'], 'network_id': ['ethereum'], 'block_num': [100], 'value': [10.0]}
            batch_eth = pa.RecordBatch.from_pydict(data_eth)
            ranges_eth = [BlockRange(network='ethereum', start=100, end=100)]
            batch_eth_with_meta = loader._add_metadata_columns(batch_eth, ranges_eth)

            data_poly = {'tx_hash': ['0x100_poly'], 'network_id': ['polygon'], 'block_num': [100], 'value': [10.0]}
            batch_poly = pa.RecordBatch.from_pydict(data_poly)
            ranges_poly = [BlockRange(network='polygon', start=100, end=100)]
            batch_poly_with_meta = loader._add_metadata_columns(batch_poly, ranges_poly)

            # Load both batches
            result1 = loader.load_batch(batch_eth_with_meta, test_table_name, create_table=True)
            result2 = loader.load_batch(batch_poly_with_meta, test_table_name, create_table=False)

            assert result1.success and result2.success

            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Verify both networks' data exists
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 2

                    # Invalidate only ethereum network
                    invalidation_ranges = [BlockRange(network='ethereum', start=100, end=100)]
                    loader._handle_reorg(invalidation_ranges, test_table_name)

                    # Should only delete ethereum data, polygon should remain
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 1

                    # Verify remaining data is from polygon
                    cur.execute(f'SELECT "_meta_block_ranges" FROM {test_table_name}')
                    remaining_ranges = cur.fetchone()[0]
                    # PostgreSQL JSONB automatically parses to Python objects
                    ranges_data = remaining_ranges
                    assert ranges_data[0]['network'] == 'polygon'

            finally:
                loader.pool.putconn(conn)
