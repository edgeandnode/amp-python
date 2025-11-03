"""
Integration tests for checkpoint and resume workflow.

Tests checkpoint storage, loading, invalidation, and end-to-end resume functionality
with real database connections.
"""

import time
from datetime import UTC, datetime

import psycopg2
import pytest

from src.amp.streaming.checkpoint import (
    CheckpointConfig,
    CheckpointState,
    DatabaseCheckpointStore,
)
from src.amp.streaming.types import BlockRange


@pytest.fixture
def checkpoint_db_connection(postgresql_test_config):
    """Create a database connection for checkpoint tests"""
    conn = psycopg2.connect(
        host=postgresql_test_config['host'],
        port=postgresql_test_config['port'],
        database=postgresql_test_config['database'],
        user=postgresql_test_config['user'],
        password=postgresql_test_config.get('password'),
    )
    yield conn
    conn.close()


@pytest.fixture
def checkpoint_store(checkpoint_db_connection):
    """Create a DatabaseCheckpointStore for testing"""
    config = CheckpointConfig(
        enabled=True,
        storage='db',
        table_prefix='test_amp_',
    )
    store = DatabaseCheckpointStore(config, checkpoint_db_connection)

    yield store

    # Cleanup: drop checkpoint table
    try:
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_checkpoints CASCADE')
        checkpoint_db_connection.commit()
    except Exception:
        pass


@pytest.mark.integration
@pytest.mark.postgresql
class TestDatabaseCheckpointStore:
    """Test checkpoint storage and retrieval with real database"""

    def test_checkpoint_table_creation(self, checkpoint_store, checkpoint_db_connection):
        """Test that checkpoint table is created automatically"""
        cursor = checkpoint_db_connection.cursor()

        # Check table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'test_amp_checkpoints'
            )
        """)
        exists = cursor.fetchone()[0]
        assert exists is True

    def test_save_and_load_checkpoint(self, checkpoint_store):
        """Test basic checkpoint save and load"""
        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc'),
            BlockRange(network='polygon', start=50, end=150, hash='0xdef'),
        ]

        checkpoint = CheckpointState(
            ranges=ranges,
            timestamp=datetime.now(UTC),
            worker_id=0,
        )

        # Save checkpoint
        checkpoint_store.save('test_connection', 'test_table', checkpoint)

        # Load checkpoint
        loaded = checkpoint_store.load('test_connection', 'test_table')

        assert loaded is not None
        assert len(loaded.ranges) == 2
        assert loaded.ranges[0].network == 'ethereum'
        assert loaded.ranges[0].hash == '0xabc'
        assert loaded.ranges[1].network == 'polygon'

    def test_checkpoint_upsert(self, checkpoint_store):
        """Test that saving same checkpoint key updates it"""
        ranges1 = [BlockRange(network='ethereum', start=100, end=200)]
        checkpoint1 = CheckpointState(
            ranges=ranges1,
            timestamp=datetime.now(UTC),
        )

        # Save first checkpoint
        checkpoint_store.save('conn1', 'table1', checkpoint1)

        # Save updated checkpoint with same key
        ranges2 = [BlockRange(network='ethereum', start=200, end=300)]
        checkpoint2 = CheckpointState(
            ranges=ranges2,
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('conn1', 'table1', checkpoint2)

        # Load should get latest
        loaded = checkpoint_store.load('conn1', 'table1')
        assert loaded.ranges[0].start == 200

    def test_multiple_workers(self, checkpoint_store):
        """Test that different workers can have separate checkpoints"""
        ranges_w0 = [BlockRange(network='ethereum', start=100, end=200)]
        checkpoint_w0 = CheckpointState(
            ranges=ranges_w0,
            timestamp=datetime.now(UTC),
            worker_id=0,
        )

        ranges_w1 = [BlockRange(network='ethereum', start=300, end=400)]
        checkpoint_w1 = CheckpointState(
            ranges=ranges_w1,
            timestamp=datetime.now(UTC),
            worker_id=1,
        )

        # Save checkpoints for different workers
        checkpoint_store.save('conn1', 'table1', checkpoint_w0)
        checkpoint_store.save('conn1', 'table1', checkpoint_w1)

        # Load worker 0 checkpoint
        loaded_w0 = checkpoint_store.load('conn1', 'table1', worker_id=0)
        assert loaded_w0.ranges[0].start == 100

        # Load worker 1 checkpoint
        loaded_w1 = checkpoint_store.load('conn1', 'table1', worker_id=1)
        assert loaded_w1.ranges[0].start == 300

    def test_delete_for_network(self, checkpoint_store):
        """Test checkpoint deletion for specific network after reorg"""
        # Create checkpoints for different networks
        checkpoint_eth = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.now(UTC),
            worker_id=0,
        )
        checkpoint_poly = CheckpointState(
            ranges=[BlockRange(network='polygon', start=50, end=150)],
            timestamp=datetime.now(UTC),
            worker_id=1,
        )
        checkpoint_mixed = CheckpointState(
            ranges=[
                BlockRange(network='ethereum', start=200, end=300),
                BlockRange(network='polygon', start=150, end=250),
            ],
            timestamp=datetime.now(UTC),
            worker_id=2,
        )

        checkpoint_store.save('conn1', 'table1', checkpoint_eth)
        checkpoint_store.save('conn1', 'table1', checkpoint_poly)
        checkpoint_store.save('conn1', 'table1', checkpoint_mixed)

        # Delete checkpoints containing ethereum ranges (should delete checkpoint_eth and checkpoint_mixed)
        checkpoint_store.delete_for_network('conn1', 'table1', 'ethereum')

        # Ethereum checkpoints should be gone
        assert checkpoint_store.load('conn1', 'table1', worker_id=0) is None  # Pure ethereum
        assert checkpoint_store.load('conn1', 'table1', worker_id=2) is None  # Mixed with ethereum

        # Polygon-only checkpoint should still exist
        loaded = checkpoint_store.load('conn1', 'table1', worker_id=1)
        assert loaded is not None
        assert loaded.ranges[0].network == 'polygon'

    def test_delete_checkpoint(self, checkpoint_store):
        """Test checkpoint deletion"""
        checkpoint = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.now(UTC),
        )

        checkpoint_store.save('conn1', 'table1', checkpoint)

        # Verify exists
        assert checkpoint_store.load('conn1', 'table1') is not None

        # Delete
        checkpoint_store.delete('conn1', 'table1')

        # Verify deleted
        assert checkpoint_store.load('conn1', 'table1') is None

    def test_load_nonexistent_checkpoint(self, checkpoint_store):
        """Test loading checkpoint that doesn't exist"""
        loaded = checkpoint_store.load('nonexistent_conn', 'nonexistent_table')
        assert loaded is None

    def test_checkpoint_to_resume_watermark(self, checkpoint_store):
        """Test converting checkpoint to resume watermark"""
        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc', prev_hash='0x123'),
            BlockRange(network='polygon', start=50, end=150, hash='0xdef'),
        ]

        checkpoint = CheckpointState(
            ranges=ranges,
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

        checkpoint_store.save('conn1', 'table1', checkpoint)
        loaded = checkpoint_store.load('conn1', 'table1')

        # Convert to watermark
        watermark = loaded.to_resume_watermark()

        assert len(watermark.ranges) == 2
        assert watermark.ranges[0]['network'] == 'ethereum'
        assert watermark.ranges[0]['hash'] == '0xabc'
        assert watermark.ranges[0]['prev_hash'] == '0x123'
        assert watermark.timestamp == '2024-01-01T12:00:00'


@pytest.mark.integration
@pytest.mark.postgresql
class TestEndToEndCheckpointResume:
    """Test end-to-end checkpoint and resume workflow"""

    def test_checkpoint_save_during_streaming(self, checkpoint_store):
        """Test checkpoint is saved correctly during streaming simulation"""
        # Simulate streaming: process multiple batches, save checkpoint on ranges_complete
        batches = [
            (False, [BlockRange(network='ethereum', start=100, end=200)]),
            (False, [BlockRange(network='ethereum', start=200, end=300)]),
            (True, [BlockRange(network='ethereum', start=300, end=400, hash='0xabc')]),  # ranges_complete
        ]

        for ranges_complete, ranges in batches:
            if ranges_complete:
                # Save checkpoint
                checkpoint = CheckpointState(
                    ranges=ranges,
                    timestamp=datetime.now(UTC),
                )
                checkpoint_store.save('streaming_conn', 'streaming_table', checkpoint)

        # Verify checkpoint was saved with last complete ranges
        loaded = checkpoint_store.load('streaming_conn', 'streaming_table')
        assert loaded is not None
        assert loaded.ranges[0].start == 300
        assert loaded.ranges[0].end == 400
        assert loaded.ranges[0].hash == '0xabc'

    def test_resume_from_checkpoint(self, checkpoint_store):
        """Test resuming stream from saved checkpoint"""
        # Step 1: Simulate initial streaming and checkpoint save
        checkpoint = CheckpointState(
            ranges=[
                BlockRange(network='ethereum', start=500, end=600, hash='0x123'),
                BlockRange(network='polygon', start=200, end=300, hash='0x456'),
            ],
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('resume_conn', 'resume_table', checkpoint)

        # Step 2: Simulate client restart - load checkpoint
        loaded = checkpoint_store.load('resume_conn', 'resume_table')
        assert loaded is not None

        # Step 3: Convert to resume watermark for server
        watermark = loaded.to_resume_watermark()
        assert len(watermark.ranges) == 2
        assert watermark.ranges[0]['network'] == 'ethereum'
        assert watermark.ranges[0]['start'] == 500

    def test_checkpoint_deleted_after_reorg(self, checkpoint_store):
        """Test that checkpoints are deleted after reorg on affected network"""
        # Save checkpoint for ethereum
        checkpoint = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('reorg_conn', 'reorg_table', checkpoint)

        # Verify checkpoint exists
        loaded = checkpoint_store.load('reorg_conn', 'reorg_table')
        assert loaded is not None

        # Simulate reorg detection - delete checkpoint for ethereum network
        checkpoint_store.delete_for_network('reorg_conn', 'reorg_table', 'ethereum')

        # Checkpoint should be deleted
        loaded_after_reorg = checkpoint_store.load('reorg_conn', 'reorg_table')
        assert loaded_after_reorg is None

        # In real code, stream would restart from beginning with idempotency preventing duplicates

    def test_multiple_checkpoint_updates_during_stream(self, checkpoint_store):
        """Test that checkpoints are updated progressively during streaming"""
        checkpoints = []

        # Simulate streaming with multiple microbatch completions
        for i in range(5):
            checkpoint = CheckpointState(
                ranges=[BlockRange(network='ethereum', start=i * 100, end=(i + 1) * 100)],
                timestamp=datetime.now(UTC),
            )
            checkpoint_store.save('progressive_conn', 'progressive_table', checkpoint)
            checkpoints.append(checkpoint)

            # Small delay to ensure different timestamps
            time.sleep(0.01)

        # Latest checkpoint should be the last one
        loaded = checkpoint_store.load('progressive_conn', 'progressive_table')
        assert loaded is not None
        assert loaded.ranges[0].start == 400
        assert loaded.ranges[0].end == 500

    def test_checkpoint_with_reorg_during_streaming(self, checkpoint_store):
        """Test checkpoint deletion when reorg occurs during streaming"""
        # Save initial checkpoint
        checkpoint_v0 = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('reorg_conn', 'reorg_table', checkpoint_v0)

        # Simulate reorg detection - delete checkpoint for ethereum
        checkpoint_store.delete_for_network('reorg_conn', 'reorg_table', 'ethereum')

        # Old checkpoint should be gone
        assert checkpoint_store.load('reorg_conn', 'reorg_table') is None

        # Save new checkpoint after reorg (stream would have restarted)
        checkpoint_v1 = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=180, end=280)],  # May overlap with old
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('reorg_conn', 'reorg_table', checkpoint_v1)

        # Should load new checkpoint
        loaded = checkpoint_store.load('reorg_conn', 'reorg_table')
        assert loaded is not None
        assert loaded.ranges[0].start == 180


@pytest.mark.integration
class TestCheckpointDisabled:
    """Test that checkpoint system can be disabled"""

    def test_checkpoint_disabled_by_default(self):
        """Test that checkpoints are disabled by default"""

        config = CheckpointConfig()
        assert config.enabled is False

        # Default loader should use NullCheckpointStore
        # (tested in unit tests, but good to verify integration)

    def test_null_checkpoint_store_no_side_effects(self):
        """Test NullCheckpointStore has no side effects"""
        from src.amp.streaming.checkpoint import NullCheckpointStore

        config = CheckpointConfig(enabled=False)
        store = NullCheckpointStore(config)

        # All operations should be no-ops
        checkpoint = CheckpointState(
            ranges=[BlockRange(network='ethereum', start=100, end=200)],
            timestamp=datetime.now(UTC),
        )

        store.save('conn1', 'table1', checkpoint)  # No-op
        loaded = store.load('conn1', 'table1')  # Returns None
        assert loaded is None

        store.delete_for_network('conn1', 'table1', 'ethereum')  # No-op
        store.delete('conn1', 'table1')  # No-op


@pytest.mark.integration
@pytest.mark.postgresql
class TestIdempotencyExactlyOnce:
    """Test exactly-once semantics with idempotency system"""

    def test_processed_ranges_table_creation(self, checkpoint_db_connection):
        """Test that processed ranges table is created automatically"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(
            enabled=True,
            table_prefix='test_amp_',
        )
        DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        cursor = checkpoint_db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'test_amp_processed_ranges'
            )
        """)
        exists = cursor.fetchone()[0]
        assert exists is True

        # Cleanup
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_mark_and_check_processed(self, checkpoint_db_connection):
        """Test marking ranges as processed and checking them"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        ranges = [
            BlockRange(network='ethereum', start=100, end=200, hash='0xabc'),
            BlockRange(network='polygon', start=50, end=150, hash='0xdef'),
        ]

        # Initially not processed
        assert store.is_processed('conn1', 'table1', ranges) is False

        # Mark as processed
        store.mark_processed('conn1', 'table1', ranges, batch_hash='hash123')

        # Now should be processed
        assert store.is_processed('conn1', 'table1', ranges) is True

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_duplicate_detection(self, checkpoint_db_connection):
        """Test that duplicates are detected correctly"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        range1 = [BlockRange(network='ethereum', start=100, end=200)]
        range2 = [BlockRange(network='ethereum', start=200, end=300)]
        range3 = [BlockRange(network='ethereum', start=100, end=200)]  # Duplicate of range1

        # Process range1
        store.mark_processed('conn1', 'table1', range1)

        # Process range2
        store.mark_processed('conn1', 'table1', range2)

        # Check duplicates
        assert store.is_processed('conn1', 'table1', range1) is True  # Duplicate
        assert store.is_processed('conn1', 'table1', range2) is True  # Duplicate
        assert store.is_processed('conn1', 'table1', range3) is True  # Same as range1

        # New range should not be processed
        range4 = [BlockRange(network='ethereum', start=300, end=400)]
        assert store.is_processed('conn1', 'table1', range4) is False

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_partial_overlap_not_detected(self, checkpoint_db_connection):
        """Test that partial overlaps are treated as different ranges"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        range1 = [BlockRange(network='ethereum', start=100, end=200)]
        range2 = [BlockRange(network='ethereum', start=150, end=250)]  # Partial overlap

        # Process range1
        store.mark_processed('conn1', 'table1', range1)

        # range2 should not be detected as duplicate (different range)
        assert store.is_processed('conn1', 'table1', range2) is False

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_cleanup_old_ranges(self, checkpoint_db_connection):
        """Test cleanup of old processed ranges"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        ranges = [BlockRange(network='ethereum', start=100, end=200)]

        # Mark as processed
        store.mark_processed('conn1', 'table1', ranges)

        # Verify it exists
        assert store.is_processed('conn1', 'table1', ranges) is True

        # Cleanup ranges older than 0 days (should delete everything)
        deleted = store.cleanup_old_ranges('conn1', 'table1', 0)
        assert deleted == 1

        # Should no longer be processed
        assert store.is_processed('conn1', 'table1', ranges) is False

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_different_connections_separate_tracking(self, checkpoint_db_connection):
        """Test that different connections track ranges separately"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        ranges = [BlockRange(network='ethereum', start=100, end=200)]

        # Mark as processed for conn1
        store.mark_processed('conn1', 'table1', ranges)

        # Should be processed for conn1
        assert store.is_processed('conn1', 'table1', ranges) is True

        # Should NOT be processed for conn2 (different connection)
        assert store.is_processed('conn2', 'table1', ranges) is False

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_different_tables_separate_tracking(self, checkpoint_db_connection):
        """Test that different tables track ranges separately"""
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')
        store = DatabaseProcessedRangesStore(config, checkpoint_db_connection)

        ranges = [BlockRange(network='ethereum', start=100, end=200)]

        # Mark as processed for table1
        store.mark_processed('conn1', 'table1', ranges)

        # Should be processed for table1
        assert store.is_processed('conn1', 'table1', ranges) is True

        # Should NOT be processed for table2 (different table)
        assert store.is_processed('conn1', 'table2', ranges) is False

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_exactly_once_with_checkpoint_integration(self, checkpoint_db_connection):
        """Test exactly-once semantics integrated with checkpoint system"""
        from src.amp.streaming.checkpoint import DatabaseCheckpointStore
        from src.amp.streaming.idempotency import DatabaseProcessedRangesStore, IdempotencyConfig

        # Setup stores
        checkpoint_config = CheckpointConfig(enabled=True, table_prefix='test_amp_')
        idempotency_config = IdempotencyConfig(enabled=True, table_prefix='test_amp_')

        checkpoint_store = DatabaseCheckpointStore(checkpoint_config, checkpoint_db_connection)
        processed_ranges_store = DatabaseProcessedRangesStore(idempotency_config, checkpoint_db_connection)

        # Simulate processing workflow
        ranges_batch1 = [BlockRange(network='ethereum', start=100, end=200, hash='0xabc')]
        ranges_batch2 = [BlockRange(network='ethereum', start=200, end=300, hash='0xdef')]

        # Process batch 1
        assert processed_ranges_store.is_processed('conn1', 'table1', ranges_batch1) is False
        processed_ranges_store.mark_processed('conn1', 'table1', ranges_batch1)

        # Save checkpoint after batch 1
        checkpoint1 = CheckpointState(
            ranges=ranges_batch1,
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('conn1', 'table1', checkpoint1)

        # Process batch 2
        assert processed_ranges_store.is_processed('conn1', 'table1', ranges_batch2) is False
        processed_ranges_store.mark_processed('conn1', 'table1', ranges_batch2)

        # Save checkpoint after batch 2
        checkpoint2 = CheckpointState(
            ranges=ranges_batch2,
            timestamp=datetime.now(UTC),
        )
        checkpoint_store.save('conn1', 'table1', checkpoint2)

        # Simulate restart: Load checkpoint
        loaded_checkpoint = checkpoint_store.load('conn1', 'table1')
        assert loaded_checkpoint is not None
        assert loaded_checkpoint.ranges[0].start == 200  # Latest checkpoint

        # Check if ranges already processed (duplicate detection)
        assert processed_ranges_store.is_processed('conn1', 'table1', ranges_batch1) is True
        assert processed_ranges_store.is_processed('conn1', 'table1', ranges_batch2) is True

        # Cleanup
        cursor = checkpoint_db_connection.cursor()
        cursor.execute('DROP TABLE IF EXISTS test_amp_checkpoints CASCADE')
        cursor.execute('DROP TABLE IF EXISTS test_amp_processed_ranges CASCADE')
        checkpoint_db_connection.commit()

    def test_transactional_exactly_once_postgresql(self, postgresql_test_config, small_test_data):
        """Test exactly-once semantics with streaming API and in-memory state"""
        from src.amp.loaders.implementations.postgresql_loader import PostgreSQLLoader
        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        loader = PostgreSQLLoader(postgresql_test_config)
        test_table = 'test_transactional_exactly_once'

        try:
            loader.connect()

            # Create table
            loader._create_table_from_schema(small_test_data.schema, test_table)

            # Create batch with metadata
            batch = small_test_data.to_batches()[0]
            ranges = [BlockRange(network='ethereum', start=100, end=200, hash='0xabc')]

            response1 = ResponseBatch.data_batch(data=batch, metadata=BatchMetadata(ranges=ranges))

            # First load - should succeed
            results = list(loader.load_stream_continuous(iter([response1]), test_table))
            assert len(results) == 1
            assert results[0].success
            assert results[0].rows_loaded == small_test_data.num_rows

            # Verify data was loaded
            conn = loader.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM {test_table}')
                count = cursor.fetchone()[0]
                assert count == small_test_data.num_rows
            finally:
                loader.pool.putconn(conn)

            # Second load with same ranges - should detect duplicate and skip
            response2 = ResponseBatch.data_batch(data=batch, metadata=BatchMetadata(ranges=ranges))
            results2 = list(loader.load_stream_continuous(iter([response2]), test_table))
            assert len(results2) == 1
            assert results2[0].success
            assert results2[0].rows_loaded == 0, 'Second load should return 0 (duplicate detected)'
            assert results2[0].metadata['operation'] == 'skip_duplicate'

            # Verify no duplicate data was inserted
            conn = loader.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM {test_table}')
                count = cursor.fetchone()[0]
                assert count == small_test_data.num_rows, 'No duplicate data should be inserted'
            finally:
                loader.pool.putconn(conn)

        finally:
            # Cleanup
            conn = loader.pool.getconn()
            try:
                cursor = conn.cursor()
                cursor.execute(f'DROP TABLE IF EXISTS {test_table} CASCADE')
                conn.commit()
            finally:
                loader.pool.putconn(conn)
            loader.disconnect()
