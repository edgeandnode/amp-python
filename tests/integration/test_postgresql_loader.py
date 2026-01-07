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
            non_meta_fields = [
                field for field in schema if not (field.name.startswith('_meta_') or field.name.startswith('_amp_'))
            ]

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
                    assert '_amp_batch_id' in column_names

                    # Verify metadata column types
                    column_types = {col[0]: col[1] for col in columns}
                    assert (
                        'text' in column_types['_amp_batch_id'].lower()
                        or 'varchar' in column_types['_amp_batch_id'].lower()
                    )

                    # Verify data was stored correctly
                    cur.execute(f'SELECT "_amp_batch_id" FROM {test_table_name} LIMIT 1')
                    meta_row = cur.fetchone()

                    # _amp_batch_id contains a compact 16-char hex string (or multiple separated by |)
                    batch_id_str = meta_row[0]
                    assert batch_id_str is not None
                    assert isinstance(batch_id_str, str)
                    assert len(batch_id_str) >= 16  # At least one 16-char batch ID

            finally:
                loader.pool.putconn(conn)

    def test_handle_reorg_deletion(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test that _handle_reorg correctly deletes invalidated ranges"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Create streaming batches with metadata
            batch1 = pa.RecordBatch.from_pydict(
                {
                    'tx_hash': ['0x100', '0x101', '0x102'],
                    'block_num': [100, 101, 102],
                    'value': [10.0, 11.0, 12.0],
                }
            )
            batch2 = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x200', '0x201'], 'block_num': [103, 104], 'value': [12.0, 33.0]}
            )
            batch3 = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x300', '0x301'], 'block_num': [105, 106], 'value': [7.0, 9.0]}
            )
            batch4 = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x400', '0x401'], 'block_num': [107, 108], 'value': [6.0, 73.0]}
            )

            # Create table from first batch schema
            loader._create_table_from_schema(batch1.schema, test_table_name)

            # Create response batches with hashes
            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=102, hash='0xaaa')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=103, end=104, hash='0xbbb')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=105, end=106, hash='0xccc')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )
            response4 = ResponseBatch.data_batch(
                data=batch4,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=107, end=108, hash='0xddd')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )

            # Load via streaming API
            stream = [response1, response2, response3, response4]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))
            assert len(results) == 4
            assert all(r.success for r in results)

            # Verify initial data count
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    initial_count = cur.fetchone()[0]
                    assert initial_count == 9  # 3 + 2 + 2 + 2

                    # Test reorg deletion - invalidate blocks 104-108 on ethereum
                    reorg_response = ResponseBatch.reorg_batch(
                        invalidation_ranges=[BlockRange(network='ethereum', start=104, end=108)]
                    )
                    reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))
                    assert len(reorg_results) == 1
                    assert reorg_results[0].success

                    # Should delete batch2, batch3 and batch4 leaving only the 3 rows from batch1
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    after_reorg_count = cur.fetchone()[0]
                    assert after_reorg_count == 3

            finally:
                loader.pool.putconn(conn)

    def test_reorg_with_overlapping_ranges(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test reorg deletion with overlapping block ranges"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Load data with overlapping ranges that should be invalidated
            batch = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x150', '0x175', '0x250'], 'block_num': [150, 175, 250], 'value': [15.0, 17.5, 25.0]}
            )

            # Create table from batch schema
            loader._create_table_from_schema(batch.schema, test_table_name)

            response = ResponseBatch.data_batch(
                data=batch,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=150, end=175, hash='0xaaa')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )

            # Load via streaming API
            results = list(loader.load_stream_continuous(iter([response]), test_table_name))
            assert len(results) == 1
            assert results[0].success

            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Verify initial data
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 3

                    # Test partial overlap invalidation (160-180)
                    # This should invalidate our range [150,175] because they overlap
                    reorg_response = ResponseBatch.reorg_batch(
                        invalidation_ranges=[BlockRange(network='ethereum', start=160, end=180)]
                    )
                    reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))
                    assert len(reorg_results) == 1
                    assert reorg_results[0].success

                    # All data should be deleted due to overlap
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 0

            finally:
                loader.pool.putconn(conn)

    def test_reorg_preserves_different_networks(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test that reorg only affects specified network"""
        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        loader = PostgreSQLLoader(postgresql_test_config)

        with loader:
            # Load data from multiple networks with same block ranges
            batch_eth = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x100_eth'], 'network_id': ['ethereum'], 'block_num': [100], 'value': [10.0]}
            )
            batch_poly = pa.RecordBatch.from_pydict(
                {'tx_hash': ['0x100_poly'], 'network_id': ['polygon'], 'block_num': [100], 'value': [10.0]}
            )

            # Create table from batch schema
            loader._create_table_from_schema(batch_eth.schema, test_table_name)

            response_eth = ResponseBatch.data_batch(
                data=batch_eth,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=100, hash='0xaaa')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )
            response_poly = ResponseBatch.data_batch(
                data=batch_poly,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='polygon', start=100, end=100, hash='0xbbb')],
                    ranges_complete=True,  # Mark as complete so it gets tracked in state store
                ),
            )

            # Load both batches via streaming API
            stream = [response_eth, response_poly]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))
            assert len(results) == 2
            assert all(r.success for r in results)

            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Verify both networks' data exists
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 2

                    # Invalidate only ethereum network
                    reorg_response = ResponseBatch.reorg_batch(
                        invalidation_ranges=[BlockRange(network='ethereum', start=100, end=100)]
                    )
                    reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))
                    assert len(reorg_results) == 1
                    assert reorg_results[0].success

                    # Should only delete ethereum data, polygon should remain
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    assert cur.fetchone()[0] == 1

            finally:
                loader.pool.putconn(conn)

    def test_microbatch_deduplication(self, postgresql_test_config, test_table_name, cleanup_tables):
        """
        Test that multiple RecordBatches within the same microbatch are all loaded,
        and deduplication only happens at microbatch boundaries when ranges_complete=True.

        This test verifies the fix for the critical bug where we were marking batches
        as processed after every RecordBatch instead of waiting for ranges_complete=True.
        """
        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        cleanup_tables.append(test_table_name)

        # Enable state management to test deduplication
        config_with_state = {
            **postgresql_test_config,
            'state': {'enabled': True, 'storage': 'memory', 'store_batch_id': True},
        }
        loader = PostgreSQLLoader(config_with_state)

        with loader:
            # Create table first from the schema
            batch1_data = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})
            loader._create_table_from_schema(batch1_data.schema, test_table_name)

            # Simulate a microbatch sent as 3 RecordBatches with the same BlockRange
            # This happens when the server sends large microbatches in smaller chunks

            # First RecordBatch of the microbatch (ranges_complete=False)
            response1 = ResponseBatch.data_batch(
                data=batch1_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],
                    ranges_complete=False,  # Not the last batch in this microbatch
                ),
            )

            # Second RecordBatch of the microbatch (ranges_complete=False)
            batch2_data = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})
            response2 = ResponseBatch.data_batch(
                data=batch2_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],  # Same BlockRange!
                    ranges_complete=False,  # Still not the last batch
                ),
            )

            # Third RecordBatch of the microbatch (ranges_complete=True)
            batch3_data = pa.RecordBatch.from_pydict({'id': [5, 6], 'value': [500, 600]})
            response3 = ResponseBatch.data_batch(
                data=batch3_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],  # Same BlockRange!
                    ranges_complete=True,  # Last batch in this microbatch - safe to mark as processed
                ),
            )

            # Process the microbatch stream
            stream = [response1, response2, response3]
            results = list(
                loader.load_stream_continuous(iter(stream), test_table_name, connection_name='test_connection')
            )

            # CRITICAL: All 3 RecordBatches should be loaded successfully
            # Before the fix, only the first batch would load (the other 2 would be skipped as "duplicates")
            assert len(results) == 3, 'All RecordBatches within microbatch should be processed'
            assert all(r.success for r in results), 'All batches should succeed'
            assert results[0].rows_loaded == 2, 'First batch should load 2 rows'
            assert results[1].rows_loaded == 2, 'Second batch should load 2 rows (not skipped!)'
            assert results[2].rows_loaded == 2, 'Third batch should load 2 rows (not skipped!)'

            # Verify total rows in table (all batches loaded)
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    total_count = cur.fetchone()[0]
                    assert total_count == 6, 'All 6 rows from 3 RecordBatches should be in the table'

                    # Verify the actual IDs are present
                    cur.execute(f'SELECT id FROM {test_table_name} ORDER BY id')
                    all_ids = [row[0] for row in cur.fetchall()]
                    assert all_ids == [1, 2, 3, 4, 5, 6], 'All rows from all RecordBatches should be present'

            finally:
                loader.pool.putconn(conn)

            # Now test that re-sending the complete microbatch is properly deduplicated
            # This time, the first batch has ranges_complete=True (entire microbatch in one RecordBatch)
            duplicate_batch = pa.RecordBatch.from_pydict({'id': [7, 8], 'value': [700, 800]})
            duplicate_response = ResponseBatch.data_batch(
                data=duplicate_batch,
                metadata=BatchMetadata(
                    ranges=[
                        BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')
                    ],  # Same range as before!
                    ranges_complete=True,  # Complete microbatch
                ),
            )

            # Process duplicate microbatch
            duplicate_results = list(
                loader.load_stream_continuous(
                    iter([duplicate_response]), test_table_name, connection_name='test_connection'
                )
            )

            # The duplicate microbatch should be skipped (already processed)
            assert len(duplicate_results) == 1
            assert duplicate_results[0].success is True
            assert duplicate_results[0].rows_loaded == 0, 'Duplicate microbatch should be skipped'
            assert duplicate_results[0].metadata.get('operation') == 'skip_duplicate', 'Should be marked as duplicate'

            # Verify row count unchanged (duplicate was skipped)
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    final_count = cur.fetchone()[0]
                    assert final_count == 6, 'Row count should not increase after duplicate microbatch'

            finally:
                loader.pool.putconn(conn)
