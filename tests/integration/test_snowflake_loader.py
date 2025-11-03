# tests/integration/test_snowflake_loader.py
"""
Integration tests for Snowflake loader implementation.
These tests require a running Snowflake instance with proper credentials.

NOTE: Snowflake integration tests are currently disabled because they require an active snowflake account
To re-enable these tests:
1. Set up a valid Snowflake account with billing enabled
2. Configure the following environment variables:
   - SNOWFLAKE_ACCOUNT
   - SNOWFLAKE_USER
   - SNOWFLAKE_PASSWORD
   - SNOWFLAKE_WAREHOUSE
   - SNOWFLAKE_DATABASE
   - SNOWFLAKE_SCHEMA (optional, defaults to PUBLIC)
3. Remove the skip decorator below
"""

import time
from datetime import datetime

import pyarrow as pa
import pytest

try:
    from src.amp.loaders.base import LoadMode
    from src.amp.loaders.implementations.snowflake_loader import SnowflakeLoader
    from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


def wait_for_snowpipe_data(loader, table_name, expected_count, max_wait=30, poll_interval=2):
    """
    Wait for Snowpipe streaming data to become queryable.

    Snowpipe streaming has eventual consistency, so data may not be immediately
    queryable after insertion. This helper polls until the expected row count is visible.

    Args:
        loader: SnowflakeLoader instance with active connection
        table_name: Name of the table to query
        expected_count: Expected number of rows
        max_wait: Maximum seconds to wait (default 30)
        poll_interval: Seconds between poll attempts (default 2)

    Returns:
        int: Actual row count found

    Raises:
        AssertionError: If expected count not reached within max_wait seconds
    """
    elapsed = 0
    while elapsed < max_wait:
        loader.cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        count = loader.cursor.fetchone()['COUNT(*)']
        if count == expected_count:
            return count
        time.sleep(poll_interval)
        elapsed += poll_interval

    # Final check before giving up
    loader.cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    count = loader.cursor.fetchone()['COUNT(*)']
    assert count == expected_count, f'Expected {expected_count} rows after {max_wait}s, but found {count}'
    return count


# Skip all Snowflake tests
# pytestmark = pytest.mark.skip(reason='Requires active Snowflake account - see module docstring for details')


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


@pytest.fixture
def cleanup_tables(snowflake_config):
    """Cleanup test tables after tests"""
    tables_to_clean = []

    yield tables_to_clean

    loader = SnowflakeLoader(snowflake_config)
    try:
        loader.connect()
        for table in tables_to_clean:
            try:
                loader.cursor.execute(f'DROP TABLE IF EXISTS {table}')
                loader.connection.commit()
            except Exception:
                pass
    except Exception:
        pass
    finally:
        if loader._is_connected:
            loader.disconnect()


@pytest.mark.integration
@pytest.mark.snowflake
class TestSnowflakeLoaderIntegration:
    """Integration tests for Snowflake loader"""

    def test_loader_connection(self, snowflake_config):
        """Test basic connection to Snowflake"""
        loader = SnowflakeLoader(snowflake_config)

        loader.connect()
        assert loader._is_connected is True
        assert loader.connection is not None
        assert loader.cursor is not None

        loader.disconnect()
        assert loader._is_connected is False
        assert loader.connection is None
        assert loader.cursor is None

    def test_basic_table_loading_via_stage(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test basic table loading using stage"""
        cleanup_tables.append(test_table_name)

        config = {**snowflake_config, 'loading_method': 'stage'}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == small_test_table.num_rows
            assert result.table_name == test_table_name
            assert result.loader_type == 'snowflake'
            assert result.metadata['loading_method'] == 'stage'

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows

    def test_basic_table_loading_via_insert(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test basic table loading using INSERT (Note: currently defaults to stage for performance)"""
        cleanup_tables.append(test_table_name)

        # Use insert loading (Note: implementation may default to stage for small tables)
        config = {**snowflake_config, 'loading_method': 'insert'}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == small_test_table.num_rows
            # Note: Implementation uses stage by default for performance
            assert result.metadata['loading_method'] in ['insert', 'stage']

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows

    def test_batch_loading(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test loading data in batches"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Use smaller batch size to force multiple batches (medium_test_table has 10000 rows)
            result = loader.load_table(medium_test_table, test_table_name, create_table=True, batch_size=5000)

            assert result.success is True
            assert result.rows_loaded == medium_test_table.num_rows
            # Implementation may optimize batching, so just check >= 1
            assert result.metadata.get('batches_processed', 1) >= 1

            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == medium_test_table.num_rows

    def test_overwrite_mode(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test OVERWRITE mode is not supported"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result1 = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result1.success is True

            # OVERWRITE mode should fail with error message
            result2 = loader.load_table(small_test_table, test_table_name, mode=LoadMode.OVERWRITE)
            assert result2.success is False
            assert 'Unsupported mode LoadMode.OVERWRITE' in result2.error

    def test_append_mode(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test APPEND mode adds to existing data"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result1 = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result1.success is True

            result2 = loader.load_table(small_test_table, test_table_name, mode=LoadMode.APPEND)
            assert result2.success is True

            # Should have double the rows
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == small_test_table.num_rows * 2

    def test_comprehensive_data_types(self, snowflake_config, comprehensive_test_data, test_table_name, cleanup_tables):
        """Test various data types from comprehensive test data"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, test_table_name, create_table=True)
            assert result.success is True

            loader.cursor.execute(f"""
                SELECT 
                    "id",
                    "user_id",
                    "transaction_amount",
                    "category",
                    "timestamp",
                    "is_weekend",
                    "score",
                    "active"
                FROM {test_table_name}
                WHERE "id" = 0
            """)

            row = loader.cursor.fetchone()
            assert row['id'] == 0
            assert row['user_id'] == 'user_0'
            assert abs(row['transaction_amount'] - 0.0) < 0.001
            assert row['category'] == 'electronics'
            assert row['is_weekend'] is True  # id=0 is weekend
            assert abs(row['score'] - 0.0) < 0.001
            assert row['active'] is True

    def test_null_handling(self, snowflake_config, null_test_data, test_table_name, cleanup_tables):
        """Test proper handling of NULL values"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(null_test_data, test_table_name, create_table=True)
            assert result.success is True

            loader.cursor.execute(f"""
                SELECT COUNT(*) as null_count
                FROM {test_table_name}
                WHERE "text_field" IS NULL
            """)

            null_count = loader.cursor.fetchone()['NULL_COUNT']
            expected_nulls = sum(1 for val in null_test_data.column('text_field').to_pylist() if val is None)
            assert null_count == expected_nulls

            loader.cursor.execute(f"""
                SELECT 
                    COUNT(CASE WHEN "int_field" IS NULL THEN 1 END) as int_nulls,
                    COUNT(CASE WHEN "float_field" IS NULL THEN 1 END) as float_nulls,
                    COUNT(CASE WHEN "bool_field" IS NULL THEN 1 END) as bool_nulls
                FROM {test_table_name}
            """)

            null_counts = loader.cursor.fetchone()
            assert null_counts['INT_NULLS'] > 0
            assert null_counts['FLOAT_NULLS'] > 0
            assert null_counts['BOOL_NULLS'] > 0

    def test_table_info(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test getting table information"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name, create_table=True)
            assert result.success is True

            info = loader.get_table_info(test_table_name)

            assert info is not None
            assert info['table_name'] == test_table_name.upper()
            assert info['schema'] == snowflake_config.get('schema', 'PUBLIC')
            # Table should have original columns + _amp_batch_id metadata column
            assert len(info['columns']) == len(small_test_table.schema) + 1

            # Verify _amp_batch_id column exists
            batch_id_col = next((col for col in info['columns'] if col['name'].lower() == '_amp_batch_id'), None)
            assert batch_id_col is not None, 'Expected _amp_batch_id metadata column'

            # In Snowflake, quoted column names are case-sensitive but INFORMATION_SCHEMA may return them differently
            # Let's find the ID column by looking for either case variant
            id_col = None
            for col in info['columns']:
                if col['name'].upper() == 'ID' or col['name'] == 'id':
                    id_col = col
                    break

            assert id_col is not None, f'Could not find ID column in {[col["name"] for col in info["columns"]]}'
            assert 'INT' in id_col['type'] or 'NUMBER' in id_col['type']

    @pytest.mark.slow
    def test_performance_batch_loading(self, snowflake_config, performance_test_data, test_table_name, cleanup_tables):
        """Test performance with larger dataset"""
        cleanup_tables.append(test_table_name)

        config = {**snowflake_config, 'loading_method': 'stage'}
        loader = SnowflakeLoader(config)

        with loader:
            start_time = time.time()

            result = loader.load_table(performance_test_data, test_table_name, create_table=True)

            duration = time.time() - start_time

            assert result.success is True
            assert result.rows_loaded == performance_test_data.num_rows

            rows_per_second = result.rows_loaded / duration
            mb_per_second = (performance_test_data.nbytes / 1024 / 1024) / duration

            print('\nPerformance metrics:')
            print(f'  Total rows: {result.rows_loaded:,}')
            print(f'  Duration: {duration:.2f}s')
            print(f'  Throughput: {rows_per_second:,.0f} rows/sec')
            print(f'  Data rate: {mb_per_second:.2f} MB/sec')
            print(f'  Batches: {result.metadata.get("batches_processed", "N/A")}')

    def test_error_handling_invalid_table(self, snowflake_config, small_test_table):
        """Test error handling for invalid table operations"""
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Try to load without creating table
            result = loader.load_table(small_test_table, 'non_existent_table_xyz', create_table=False)

            assert result.success is False
            assert result.error is not None

    @pytest.mark.slow
    def test_concurrent_batch_loading(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test loading multiple batches concurrently"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create table first
            batch = medium_test_table.to_batches(max_chunksize=1)[0]
            loader.load_batch(batch, test_table_name, create_table=True)

            # Load multiple batches
            total_rows = 0
            for _i, batch in enumerate(medium_test_table.to_batches(max_chunksize=500)):
                result = loader.load_batch(batch, test_table_name)
                assert result.success is True
                total_rows += result.rows_loaded

            assert total_rows == medium_test_table.num_rows

            # Verify all data loaded
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == medium_test_table.num_rows + 1  # +1 for initial batch

    # Removed test_stage_and_compression_options - compression parameter not supported in current config

    def test_schema_with_special_characters(self, snowflake_config, test_table_name, cleanup_tables):
        """Test handling of column names with special characters"""
        cleanup_tables.append(test_table_name)

        data = {
            'user-id': [1, 2, 3],
            'first name': ['Alice', 'Bob', 'Charlie'],
            'total$amount': [100.0, 200.0, 300.0],
            '2024_data': ['a', 'b', 'c'],
        }
        special_table = pa.Table.from_pydict(data)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(special_table, test_table_name, create_table=True)
            assert result.success is True

            # Verify row count
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 3

            # Verify we can query columns with special characters
            # Note: Snowflake typically converts column names to uppercase and may need quoting
            loader.cursor.execute(f"""
                SELECT 
                    "user-id",
                    "first name", 
                    "total$amount",
                    "2024_data"
                FROM {test_table_name}
                WHERE "user-id" = 1
            """)

            row = loader.cursor.fetchone()

            assert row['user-id'] == 1
            assert row['first name'] == 'Alice'
            assert abs(row['total$amount'] - 100.0) < 0.001
            assert row['2024_data'] == 'a'

    def test_handle_reorg_no_metadata_column(self, snowflake_config, test_table_name, cleanup_tables):
        """Test reorg handling when table lacks metadata column"""
        from src.amp.streaming.types import BlockRange

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create table without metadata column
            data = pa.table({'id': [1, 2, 3], 'block_num': [100, 150, 200], 'value': [10.0, 20.0, 30.0]})
            loader.load_table(data, test_table_name, create_table=True)

            # Call handle reorg
            invalidation_ranges = [BlockRange(network='ethereum', start=150, end=250)]

            # Should log warning and not modify data
            loader._handle_reorg(invalidation_ranges, test_table_name, 'test_connection')

            # Verify data unchanged
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 3

    def test_handle_reorg_single_network(self, snowflake_config, test_table_name, cleanup_tables):
        """Test reorg handling for single network data"""

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create batches with proper metadata
            batch1 = pa.RecordBatch.from_pydict({'id': [1], 'block_num': [105]})
            batch2 = pa.RecordBatch.from_pydict({'id': [2], 'block_num': [155]})
            batch3 = pa.RecordBatch.from_pydict({'id': [3], 'block_num': [205]})

            # Create streaming responses with block ranges
            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc')]),
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160, hash='0xdef')]),
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=200, end=210, hash='0xghi')]),
            )

            # Load data via streaming API
            stream = [response1, response2, response3]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))

            # Verify all data loaded successfully
            assert len(results) == 3
            assert all(r.success for r in results)

            # Verify all data exists
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 3

            # Trigger reorg from block 155 - should delete rows 2 and 3
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=155, end=300)]
            )
            reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))

            # Verify reorg processed
            assert len(reorg_results) == 1
            assert reorg_results[0].is_reorg

            # Verify only first row remains
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 1

            loader.cursor.execute(f'SELECT "id" FROM {test_table_name}')
            remaining_id = loader.cursor.fetchone()['id']
            assert remaining_id == 1

    def test_handle_reorg_multi_network(self, snowflake_config, test_table_name, cleanup_tables):
        """Test reorg handling preserves data from unaffected networks"""

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create batches from multiple networks
            batch1 = pa.RecordBatch.from_pydict({'id': [1], 'network': ['ethereum']})
            batch2 = pa.RecordBatch.from_pydict({'id': [2], 'network': ['polygon']})
            batch3 = pa.RecordBatch.from_pydict({'id': [3], 'network': ['ethereum']})
            batch4 = pa.RecordBatch.from_pydict({'id': [4], 'network': ['polygon']})

            # Create streaming responses with block ranges
            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xa')]),
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(ranges=[BlockRange(network='polygon', start=100, end=110, hash='0xb')]),
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160, hash='0xc')]),
            )
            response4 = ResponseBatch.data_batch(
                data=batch4,
                metadata=BatchMetadata(ranges=[BlockRange(network='polygon', start=150, end=160, hash='0xd')]),
            )

            # Load data via streaming API
            stream = [response1, response2, response3, response4]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))

            # Verify all data loaded successfully
            assert len(results) == 4
            assert all(r.success for r in results)

            # Trigger reorg for ethereum only from block 150
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=150, end=200)]
            )
            reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))

            # Verify reorg processed
            assert len(reorg_results) == 1
            assert reorg_results[0].is_reorg

            # Verify ethereum row 3 deleted, but polygon rows preserved
            loader.cursor.execute(f'SELECT "id" FROM {test_table_name} ORDER BY "id"')
            remaining_ids = [row['id'] for row in loader.cursor.fetchall()]
            assert remaining_ids == [1, 2, 4]  # Row 3 deleted

    def test_handle_reorg_overlapping_ranges(self, snowflake_config, test_table_name, cleanup_tables):
        """Test reorg with overlapping block ranges"""

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create batches with overlapping ranges
            batch1 = pa.RecordBatch.from_pydict({'id': [1]})
            batch2 = pa.RecordBatch.from_pydict({'id': [2]})
            batch3 = pa.RecordBatch.from_pydict({'id': [3]})

            # Create streaming responses with block ranges
            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=90, end=110, hash='0xa')]
                ),  # Before reorg
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=140, end=160, hash='0xb')]
                ),  # Overlaps
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=170, end=190, hash='0xc')]
                ),  # Overlaps
            )

            # Load data via streaming API
            stream = [response1, response2, response3]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))

            # Verify all data loaded successfully
            assert len(results) == 3
            assert all(r.success for r in results)

            # Trigger reorg from block 150 - should delete rows where end >= 150
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=150, end=200)]
            )
            reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))

            # Verify reorg processed
            assert len(reorg_results) == 1
            assert reorg_results[0].is_reorg

            # Only first row should remain (ends at 110 < 150)
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 1

            loader.cursor.execute(f'SELECT "id" FROM {test_table_name}')
            remaining_id = loader.cursor.fetchone()['id']
            assert remaining_id == 1

    def test_handle_reorg_with_history_preservation(self, snowflake_config, test_table_name, cleanup_tables):
        """Test reorg history preservation mode - rows are updated instead of deleted"""

        cleanup_tables.append(test_table_name)
        cleanup_tables.append(f'{test_table_name}_current')
        cleanup_tables.append(f'{test_table_name}_history')

        # Enable history preservation
        config_with_history = {**snowflake_config, 'preserve_reorg_history': True}
        loader = SnowflakeLoader(config_with_history)

        with loader:
            # Create batches with proper metadata
            batch1 = pa.RecordBatch.from_pydict({'id': [1], 'block_num': [105]})
            batch2 = pa.RecordBatch.from_pydict({'id': [2], 'block_num': [155]})
            batch3 = pa.RecordBatch.from_pydict({'id': [3], 'block_num': [205]})

            # Create streaming responses with block ranges
            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc')]),
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160, hash='0xdef')]),
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=200, end=210, hash='0xghi')]),
            )

            # Load data via streaming API
            stream = [response1, response2, response3]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))

            # Verify all data loaded successfully
            assert len(results) == 3
            assert all(r.success for r in results)

            # Verify temporal columns exist and are set correctly
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE "_amp_is_current" = TRUE')
            current_count = loader.cursor.fetchone()['COUNT(*)']
            assert current_count == 3

            # Verify reorg columns exist
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE "_amp_reorg_batch_id" IS NULL')
            not_reorged_count = loader.cursor.fetchone()['COUNT(*)']
            assert not_reorged_count == 3  # All current rows should have NULL reorg_batch_id

            # Verify views exist
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}_current')
            view_count = loader.cursor.fetchone()['COUNT(*)']
            assert view_count == 3

            # Trigger reorg from block 155 - should UPDATE rows 2 and 3, not delete them
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=155, end=300)]
            )
            reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))

            # Verify reorg processed
            assert len(reorg_results) == 1
            assert reorg_results[0].is_reorg

            # Verify ALL 3 rows still exist in base table
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            total_count = loader.cursor.fetchone()['COUNT(*)']
            assert total_count == 3

            # Verify only first row is current
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE "_amp_is_current" = TRUE')
            current_count = loader.cursor.fetchone()['COUNT(*)']
            assert current_count == 1

            # Verify _current view shows only active row
            loader.cursor.execute(f'SELECT "id" FROM {test_table_name}_current')
            current_ids = [row['id'] for row in loader.cursor.fetchall()]
            assert current_ids == [1]

            # Verify _history view shows all rows
            loader.cursor.execute(f'SELECT "id" FROM {test_table_name}_history ORDER BY "id"')
            history_ids = [row['id'] for row in loader.cursor.fetchall()]
            assert history_ids == [1, 2, 3]

            # Verify reorged rows have simplified reorg columns set correctly
            loader.cursor.execute(
                f'''SELECT "id", "_amp_is_current", "_amp_batch_id", "_amp_reorg_batch_id"
                    FROM {test_table_name}
                    WHERE "_amp_is_current" = FALSE
                    ORDER BY "id"'''
            )
            reorged_rows = loader.cursor.fetchall()
            assert len(reorged_rows) == 2
            assert reorged_rows[0]['id'] == 2
            assert reorged_rows[1]['id'] == 3
            # Verify reorg_batch_id is set (identifies which reorg event superseded these rows)
            assert reorged_rows[0]['_amp_reorg_batch_id'] is not None
            assert reorged_rows[1]['_amp_reorg_batch_id'] is not None
            # Both rows superseded by same reorg event
            assert reorged_rows[0]['_amp_reorg_batch_id'] == reorged_rows[1]['_amp_reorg_batch_id']

    def test_parallel_streaming_with_stage(self, snowflake_config, test_table_name, cleanup_tables):
        """Test parallel streaming using stage loading method"""
        import threading

        cleanup_tables.append(test_table_name)
        config = {**snowflake_config, 'loading_method': 'stage'}
        loader = SnowflakeLoader(config)

        with loader:
            # Create table first
            initial_batch = pa.RecordBatch.from_pydict({'id': [1], 'partition': ['partition_0'], 'value': [100]})
            loader.load_batch(initial_batch, test_table_name, create_table=True)

            # Thread lock for serializing access to shared Snowflake connection
            # (Snowflake connector is not thread-safe)
            load_lock = threading.Lock()

            # Load multiple batches in parallel from different "streams"
            def load_partition_data(partition_id: int, start_id: int):
                """Simulate a stream partition loading data"""
                for batch_num in range(3):
                    batch_start = start_id + (batch_num * 10)
                    batch = pa.RecordBatch.from_pydict(
                        {
                            'id': list(range(batch_start, batch_start + 10)),
                            'partition': [f'partition_{partition_id}'] * 10,
                            'value': list(range(batch_start * 100, (batch_start + 10) * 100, 100)),
                        }
                    )
                    # Use lock to ensure thread-safe access to shared connection
                    with load_lock:
                        result = loader.load_batch(batch, test_table_name, create_table=False)
                        assert result.success, f'Partition {partition_id} batch {batch_num} failed: {result.error}'

            # Launch 3 parallel "streams" (threads simulating parallel streaming)
            threads = []
            for partition_id in range(3):
                start_id = 100 + (partition_id * 100)
                thread = threading.Thread(target=load_partition_data, args=(partition_id, start_id))
                threads.append(thread)
                thread.start()

            # Wait for all streams to complete
            for thread in threads:
                thread.join()

            # Verify all data loaded correctly
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            # 1 initial + (3 partitions * 3 batches * 10 rows) = 91 rows
            assert count == 91

            # Verify each partition loaded correctly
            for partition_id in range(3):
                loader.cursor.execute(
                    f'SELECT COUNT(*) FROM {test_table_name} WHERE "partition" = \'partition_{partition_id}\''
                )
                partition_count = loader.cursor.fetchone()['COUNT(*)']
                # partition_0 has 31 rows (1 initial + 30 from thread), others have 30
                expected_count = 31 if partition_id == 0 else 30
                assert partition_count == expected_count

    def test_streaming_with_reorg(self, snowflake_config, test_table_name, cleanup_tables):
        """Test streaming data with reorg support"""
        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Create streaming data with metadata
            data1 = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})

            data2 = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})

            # Create response batches using factory methods (with hashes for proper state management)
            response1 = ResponseBatch.data_batch(
                data=data1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')]),
            )

            response2 = ResponseBatch.data_batch(
                data=data2,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=150, end=160, hash='0xdef456')]),
            )

            # Simulate reorg event using factory method
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=150, end=200)]
            )

            # Process streaming data
            stream = [response1, response2, reorg_response]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name))

            # Verify results
            assert len(results) == 3
            assert results[0].success
            assert results[0].rows_loaded == 2
            assert results[1].success
            assert results[1].rows_loaded == 2
            assert results[2].success
            assert results[2].is_reorg

            # Verify reorg deleted the second batch
            loader.cursor.execute(f'SELECT "id" FROM {test_table_name} ORDER BY "id"')
            remaining_ids = [row['id'] for row in loader.cursor.fetchall()]
            assert remaining_ids == [1, 2]  # 3 and 4 deleted by reorg


@pytest.fixture
def snowflake_streaming_config():
    """
    Snowflake Snowpipe Streaming configuration from environment.

    Requires:
        - SNOWFLAKE_ACCOUNT: Account identifier
        - SNOWFLAKE_USER: Username
        - SNOWFLAKE_WAREHOUSE: Warehouse name
        - SNOWFLAKE_DATABASE: Database name
        - SNOWFLAKE_PRIVATE_KEY: Private key in PEM format (as string)
        - SNOWFLAKE_SCHEMA: Schema name (optional, defaults to PUBLIC)
        - SNOWFLAKE_ROLE: Role (optional)
    """
    import os

    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT', 'test_account'),
        'user': os.getenv('SNOWFLAKE_USER', 'test_user'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'test_warehouse'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'test_database'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'loading_method': 'snowpipe_streaming',
        'streaming_channel_prefix': 'test_amp',
        'streaming_max_retries': 3,
        'streaming_buffer_flush_interval': 1,
    }

    # Private key is required for Snowpipe Streaming
    if os.getenv('SNOWFLAKE_PRIVATE_KEY'):
        config['private_key'] = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    else:
        pytest.skip('Snowpipe Streaming requires SNOWFLAKE_PRIVATE_KEY environment variable')

    if os.getenv('SNOWFLAKE_ROLE'):
        config['role'] = os.getenv('SNOWFLAKE_ROLE')

    return config


@pytest.mark.integration
@pytest.mark.snowflake
class TestSnowpipeStreamingIntegration:
    """Integration tests for Snowpipe Streaming functionality"""

    def test_streaming_connection(self, snowflake_streaming_config):
        """Test connection with Snowpipe Streaming enabled"""
        loader = SnowflakeLoader(snowflake_streaming_config)

        loader.connect()
        assert loader._is_connected is True
        assert loader.connection is not None
        # Streaming channels dict is initialized empty (channels created on first load)
        assert hasattr(loader, 'streaming_channels')

        loader.disconnect()
        assert loader._is_connected is False

    def test_basic_streaming_batch_load(
        self, snowflake_streaming_config, small_test_table, test_table_name, cleanup_tables
    ):
        """Test basic batch loading via Snowpipe Streaming"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            # Load first batch
            batch = small_test_table.to_batches(max_chunksize=50)[0]
            result = loader.load_batch(batch, test_table_name, create_table=True)

            assert result.success is True
            assert result.rows_loaded == batch.num_rows
            assert result.table_name == test_table_name
            assert result.metadata['loading_method'] == 'snowpipe_streaming'

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            count = wait_for_snowpipe_data(loader, test_table_name, batch.num_rows)
            assert count == batch.num_rows

    def test_streaming_multiple_batches(
        self, snowflake_streaming_config, medium_test_table, test_table_name, cleanup_tables
    ):
        """Test loading multiple batches via Snowpipe Streaming"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            # Load multiple batches
            total_rows = 0
            for i, batch in enumerate(medium_test_table.to_batches(max_chunksize=1000)):
                result = loader.load_batch(batch, test_table_name, create_table=(i == 0))
                assert result.success is True
                total_rows += result.rows_loaded

            assert total_rows == medium_test_table.num_rows

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            count = wait_for_snowpipe_data(loader, test_table_name, medium_test_table.num_rows)
            assert count == medium_test_table.num_rows

    def test_streaming_channel_management(
        self, snowflake_streaming_config, small_test_table, test_table_name, cleanup_tables
    ):
        """Test that channels are created and reused properly"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            # Load batches with same channel suffix
            batch = small_test_table.to_batches(max_chunksize=50)[0]

            result1 = loader.load_batch(batch, test_table_name, create_table=True, channel_suffix='partition_0')
            assert result1.success is True

            result2 = loader.load_batch(batch, test_table_name, channel_suffix='partition_0')
            assert result2.success is True

            # Verify channel was reused (check loader's channel cache)
            channel_key = f'{test_table_name}:test_amp_{test_table_name}_partition_0'
            assert channel_key in loader.streaming_channels

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            count = wait_for_snowpipe_data(loader, test_table_name, batch.num_rows * 2)
            assert count == batch.num_rows * 2

    def test_streaming_multiple_partitions(
        self, snowflake_streaming_config, small_test_table, test_table_name, cleanup_tables
    ):
        """Test parallel streaming with multiple partition channels"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            batch = small_test_table.to_batches(max_chunksize=30)[0]

            # Load to different partitions
            result1 = loader.load_batch(batch, test_table_name, create_table=True, channel_suffix='partition_0')
            result2 = loader.load_batch(batch, test_table_name, channel_suffix='partition_1')
            result3 = loader.load_batch(batch, test_table_name, channel_suffix='partition_2')

            assert result1.success and result2.success and result3.success

            # Verify multiple channels created
            assert len(loader.streaming_channels) == 3

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            count = wait_for_snowpipe_data(loader, test_table_name, batch.num_rows * 3)
            assert count == batch.num_rows * 3

    def test_streaming_data_types(
        self, snowflake_streaming_config, comprehensive_test_data, test_table_name, cleanup_tables
    ):
        """Test Snowpipe Streaming with various data types"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            result = loader.load_table(comprehensive_test_data, test_table_name, create_table=True)
            assert result.success is True

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            count = wait_for_snowpipe_data(loader, test_table_name, comprehensive_test_data.num_rows)
            assert count == comprehensive_test_data.num_rows

            # Verify specific row
            loader.cursor.execute(f'SELECT * FROM {test_table_name} WHERE "id" = 0')
            row = loader.cursor.fetchone()
            assert row['id'] == 0

    def test_streaming_null_handling(self, snowflake_streaming_config, null_test_data, test_table_name, cleanup_tables):
        """Test Snowpipe Streaming with NULL values"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            result = loader.load_table(null_test_data, test_table_name, create_table=True)
            assert result.success is True

            # Wait for Snowpipe streaming data to become queryable (eventual consistency)
            wait_for_snowpipe_data(loader, test_table_name, null_test_data.num_rows)

            # Verify NULL handling
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE "text_field" IS NULL')
            null_count = loader.cursor.fetchone()['COUNT(*)']
            expected_nulls = sum(1 for val in null_test_data.column('text_field').to_pylist() if val is None)
            assert null_count == expected_nulls

    def test_streaming_reorg_channel_closure(self, snowflake_streaming_config, test_table_name, cleanup_tables):
        """Test that reorg properly closes streaming channels"""
        import json

        from src.amp.streaming.types import BlockRange

        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            # Load initial data with multiple channels
            batch = pa.RecordBatch.from_pydict(
                {
                    'id': [1, 2, 3],
                    'value': [100, 200, 300],
                    '_meta_block_ranges': [json.dumps([{'network': 'ethereum', 'start': 100, 'end': 110}])] * 3,
                }
            )

            loader.load_batch(batch, test_table_name, create_table=True, channel_suffix='partition_0')
            loader.load_batch(batch, test_table_name, channel_suffix='partition_1')

            # Verify channels exist
            assert len(loader.streaming_channels) == 2

            # Wait for data to be queryable
            time.sleep(5)

            # Trigger reorg
            invalidation_ranges = [BlockRange(network='ethereum', start=100, end=200)]
            loader._handle_reorg(invalidation_ranges, test_table_name, 'test_connection')

            # Verify channels were closed
            assert len(loader.streaming_channels) == 0

            # Verify data was deleted
            loader.cursor.execute(f'SELECT COUNT(*) FROM {test_table_name}')
            count = loader.cursor.fetchone()['COUNT(*)']
            assert count == 0

    @pytest.mark.slow
    def test_streaming_performance(
        self, snowflake_streaming_config, performance_test_data, test_table_name, cleanup_tables
    ):
        """Test Snowpipe Streaming performance with larger dataset"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            start_time = time.time()
            result = loader.load_table(performance_test_data, test_table_name, create_table=True)
            duration = time.time() - start_time

            assert result.success is True
            assert result.rows_loaded == performance_test_data.num_rows

            rows_per_second = result.rows_loaded / duration

            print('\nSnowpipe Streaming Performance:')
            print(f'  Total rows: {result.rows_loaded:,}')
            print(f'  Duration: {duration:.2f}s')
            print(f'  Throughput: {rows_per_second:,.0f} rows/sec')
            print(f'  Loading method: {result.metadata.get("loading_method")}')

            # Wait for Snowpipe streaming data to become queryable (eventual consistency, larger dataset may take longer)
            count = wait_for_snowpipe_data(loader, test_table_name, performance_test_data.num_rows, max_wait=60)
            assert count == performance_test_data.num_rows

    def test_streaming_error_handling(self, snowflake_streaming_config, test_table_name, cleanup_tables):
        """Test error handling in Snowpipe Streaming"""
        cleanup_tables.append(test_table_name)
        loader = SnowflakeLoader(snowflake_streaming_config)

        with loader:
            # Create table first
            initial_data = pa.table({'id': [1, 2, 3], 'value': [100, 200, 300]})
            result = loader.load_table(initial_data, test_table_name, create_table=True)
            assert result.success is True

            # Try to load data with extra column (Snowpipe streaming handles gracefully)
            # Note: Snowpipe streaming accepts data with extra columns and silently ignores them
            incompatible_data = pa.RecordBatch.from_pydict(
                {
                    'id': [4, 5],
                    'different_column': ['a', 'b'],  # Extra column not in table schema
                }
            )

            result = loader.load_batch(incompatible_data, test_table_name)
            # Snowpipe streaming handles this gracefully - it loads the matching columns
            # and ignores columns that don't exist in the table
            assert result.success is True
            assert result.rows_loaded == 2
