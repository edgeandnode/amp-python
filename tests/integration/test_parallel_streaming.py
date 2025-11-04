"""
Integration tests for parallel streaming functionality.

These tests require:
1. A running Amp server with streaming query support
2. A PostgreSQL database for loading results
"""

import os

import pytest

try:
    from src.amp.client import Client
    from src.amp.streaming.parallel import ParallelConfig
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


# Amp server configuration from environment variables
@pytest.fixture(scope='session')
def amp_server_url():
    """Get Amp server URL from environment"""
    url = os.getenv('AMP_SERVER_URL')
    if not url:
        pytest.skip('AMP_SERVER_URL not configured')
    return url


@pytest.fixture(scope='session')
def amp_source_table():
    """Get source table name from Amp server (e.g., 'eth_firehose.blocks')"""
    return os.getenv('AMP_TEST_TABLE', 'blocks')


@pytest.fixture(scope='session')
def amp_max_block():
    """Get max block for testing from environment"""
    return int(os.getenv('AMP_TEST_MAX_BLOCK', '1000'))


@pytest.fixture(scope='session')
def amp_client(amp_server_url):
    """Create Amp client for testing"""
    return Client(amp_server_url)


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    from datetime import datetime

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_parallel_{timestamp}'


@pytest.fixture
def cleanup_tables(postgresql_test_config):
    """Cleanup test tables after tests"""
    from src.amp.loaders.implementations.postgresql_loader import PostgreSQLLoader

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
@pytest.mark.slow
class TestParallelStreamingIntegration:
    """Integration tests for parallel streaming with real Amp server"""

    def test_parallel_historical_load(
        self, amp_client, postgresql_test_config, test_table_name, cleanup_tables, amp_source_table, amp_max_block
    ):
        """Test parallel loading of historical block range

        Loads data from amp_source_table (Amp server) to test_table_name (PostgreSQL)
        """
        cleanup_tables.append(test_table_name)

        # Configure PostgreSQL connection
        amp_client.configure_connection(name='test_postgres', loader='postgresql', config=postgresql_test_config)

        # Configure parallel execution for specific historical range
        parallel_config = ParallelConfig(
            num_workers=4, table_name=amp_source_table, min_block=0, max_block=amp_max_block, block_column='block_num'
        )

        # Execute parallel streaming query
        query = f'SELECT * FROM {amp_source_table}'
        results = amp_client.sql(query).load(
            connection='test_postgres', destination=test_table_name, stream=True, parallel_config=parallel_config
        )

        # Collect results from all partitions
        partition_results = []
        total_rows = 0
        for result in results:
            assert result.success, f'Load failed: {result.error}'
            partition_results.append(result)
            total_rows += result.rows_loaded

            # Verify result has partition metadata
            assert 'partition_id' in result.metadata
            print(f'Partition {result.metadata["partition_id"]}: {result.rows_loaded:,} rows')

        # Should have 4 partitions
        assert len(partition_results) == 4
        assert total_rows > 0

        # Verify data was loaded to PostgreSQL
        from src.amp.loaders.implementations.postgresql_loader import PostgreSQLLoader

        loader = PostgreSQLLoader(postgresql_test_config)
        with loader:
            conn = loader.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                    count = cur.fetchone()[0]
                    assert count == total_rows
            finally:
                loader.pool.putconn(conn)

    def test_hybrid_streaming_mode(
        self, amp_client, postgresql_test_config, test_table_name, cleanup_tables, amp_source_table
    ):
        """Test hybrid mode: parallel catch-up + continuous streaming"""
        from src.amp.streaming.parallel import ParallelStreamExecutor

        cleanup_tables.append(test_table_name)

        # First detect the current max block
        detect_config = ParallelConfig(
            num_workers=1,
            table_name=amp_source_table,
            block_column='block_num',
        )
        executor = ParallelStreamExecutor(amp_client, detect_config)
        current_max_block = executor._detect_current_max_block()

        # Start from close to current max to keep test data small
        # Load last ~200 blocks in parallel, then transition to continuous streaming
        min_block = max(0, current_max_block - 200)

        print(f'Current max block: {current_max_block:,}, will load from block {min_block:,}')

        # Configure PostgreSQL connection
        amp_client.configure_connection(name='test_postgres', loader='postgresql', config=postgresql_test_config)

        # Configure hybrid streaming (max_block=None enables continuous streaming after parallel catchup)
        parallel_config = ParallelConfig(
            num_workers=2,
            table_name=amp_source_table,
            min_block=min_block,
            max_block=None,  # None = auto-detect and transition to continuous streaming
            block_column='block_num',
        )

        # Execute hybrid streaming query
        query = f'SELECT * FROM {amp_source_table}'
        results = amp_client.sql(query).load(
            connection='test_postgres', destination=test_table_name, stream=True, parallel_config=parallel_config
        )

        # Collect results from both phases
        parallel_results = []
        continuous_results = []

        for i, result in enumerate(results):
            assert result.success, f'Load failed: {result.error}'

            if 'partition_id' in result.metadata:
                # Parallel catch-up phase
                parallel_results.append(result)
                print(f'Catch-up partition {result.metadata["partition_id"]}: {result.rows_loaded:,} rows')
            else:
                # Continuous streaming phase
                continuous_results.append(result)
                print(f'Live stream batch: {result.rows_loaded:,} rows')

            # Stop after seeing first continuous stream batch
            # (Don't wait indefinitely for new blocks)
            if len(continuous_results) > 0:
                break

            # Safety: stop after reasonable number of results
            if i > 100:
                break

        # Should have parallel results from catch-up phase
        assert len(parallel_results) >= 2, 'Should have at least 2 parallel partitions'

        # May or may not have continuous results depending on if new blocks arrived
        print(f'Parallel catch-up: {len(parallel_results)} partitions')
        print(f'Live streaming: {len(continuous_results)} batches')

    def test_block_detection(self, amp_client, amp_source_table):
        """Test that max block detection works correctly"""
        from src.amp.streaming.parallel import ParallelStreamExecutor

        # Create config with table name from Amp server
        config = ParallelConfig(
            num_workers=1,
            table_name=amp_source_table,
            block_column='block_num',
        )

        # Create executor and detect max block
        executor = ParallelStreamExecutor(amp_client, config)
        max_block = executor._detect_current_max_block()

        assert max_block > 0
        print(f'Detected max block: {max_block:,}')

    def test_custom_partition_size(
        self, amp_client, postgresql_test_config, test_table_name, cleanup_tables, amp_source_table, amp_max_block
    ):
        """Test parallel streaming with custom partition size"""
        cleanup_tables.append(test_table_name)

        # Configure PostgreSQL connection
        amp_client.configure_connection(name='test_postgres', loader='postgresql', config=postgresql_test_config)

        # Calculate partition size: with 4 workers, divide amp_max_block by 4
        partition_size = amp_max_block // 4

        # Configure with custom partition size
        parallel_config = ParallelConfig(
            num_workers=4,
            table_name=amp_source_table,
            min_block=0,
            max_block=amp_max_block,
            partition_size=partition_size,
            block_column='block_num',
        )

        # Execute parallel streaming query
        query = f'SELECT * FROM {amp_source_table}'
        results = amp_client.sql(query).load(
            connection='test_postgres', destination=test_table_name, stream=True, parallel_config=parallel_config
        )

        # Collect results
        partition_results = list(results)

        # With 1000 blocks and 250 per partition, should have 4 partitions
        assert len(partition_results) == 4

        for result in partition_results:
            assert result.success
            assert 'partition_id' in result.metadata
