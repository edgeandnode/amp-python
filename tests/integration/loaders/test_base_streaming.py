"""
Generalized streaming and reorg tests that work across all loader implementations.

These tests use the LoaderTestConfig abstraction to run identical streaming test logic
across different storage backends (PostgreSQL, Redis, Snowflake, etc.).
"""

import pyarrow as pa
import pytest

from tests.integration.loaders.conftest import LoaderTestBase


class BaseStreamingTests(LoaderTestBase):
    """
    Base test class with streaming and reorg functionality tests.

    Loaders that support streaming should inherit from this class and provide a LoaderTestConfig.

    Example:
        class TestPostgreSQLStreaming(BaseStreamingTests):
            config = PostgreSQLTestConfig()
    """

    @pytest.mark.integration
    def test_streaming_metadata_columns(self, loader, test_table_name, cleanup_tables):
        """Test that streaming data creates tables with metadata columns"""
        if not self.config.supports_streaming:
            pytest.skip('Loader does not support streaming')

        cleanup_tables.append(test_table_name)

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

        with loader:
            # Add metadata columns (simulating what load_stream_continuous does)
            batch_with_metadata = loader._add_metadata_columns(batch, block_ranges)

            # Load the batch
            result = loader.load_batch(batch_with_metadata, test_table_name, create_table=True)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify metadata columns were created using backend-specific method
            column_names = self.config.get_column_names(loader, test_table_name)

            # Should have original columns plus metadata columns
            assert '_amp_batch_id' in column_names

            # Verify data was loaded
            row_count = self.config.get_row_count(loader, test_table_name)
            assert row_count == 3

    @pytest.mark.integration
    def test_reorg_deletion(self, loader, test_table_name, cleanup_tables):
        """Test that _handle_reorg correctly deletes invalidated ranges"""
        if not self.config.supports_streaming:
            pytest.skip('Loader does not support streaming')

        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

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
                    ranges_complete=True,  # Mark as complete so state tracking works
                ),
            )
            response2 = ResponseBatch.data_batch(
                data=batch2,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=103, end=104, hash='0xbbb')],
                    ranges_complete=True,
                ),
            )
            response3 = ResponseBatch.data_batch(
                data=batch3,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=105, end=106, hash='0xccc')],
                    ranges_complete=True,
                ),
            )
            response4 = ResponseBatch.data_batch(
                data=batch4,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=107, end=108, hash='0xddd')],
                    ranges_complete=True,
                ),
            )

            # Load via streaming API (with connection_name for state tracking)
            stream = [response1, response2, response3, response4]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name, connection_name='test_conn'))
            assert len(results) == 4
            assert all(r.success for r in results)

            # Verify initial data count
            initial_count = self.config.get_row_count(loader, test_table_name)
            assert initial_count == 9  # 3 + 2 + 2 + 2

            # Test reorg deletion - invalidate blocks 104-108 on ethereum
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=104, end=108)]
            )
            reorg_results = list(
                loader.load_stream_continuous(iter([reorg_response]), test_table_name, connection_name='test_conn')
            )
            assert len(reorg_results) == 1
            assert reorg_results[0].success

            # Should delete batch2, batch3 and batch4 leaving only the 3 rows from batch1
            after_reorg_count = self.config.get_row_count(loader, test_table_name)
            assert after_reorg_count == 3

    @pytest.mark.integration
    def test_reorg_overlapping_ranges(self, loader, test_table_name, cleanup_tables):
        """Test reorg deletion with overlapping block ranges"""
        if not self.config.supports_streaming:
            pytest.skip('Loader does not support streaming')

        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

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
                    ranges_complete=True,
                ),
            )

            # Load via streaming API (with connection_name for state tracking)
            results = list(
                loader.load_stream_continuous(iter([response]), test_table_name, connection_name='test_conn')
            )
            assert len(results) == 1
            assert results[0].success

            # Verify initial data
            initial_count = self.config.get_row_count(loader, test_table_name)
            assert initial_count == 3

            # Test partial overlap invalidation (160-180)
            # This should invalidate our range [150,175] because they overlap
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=160, end=180)]
            )
            reorg_results = list(
                loader.load_stream_continuous(iter([reorg_response]), test_table_name, connection_name='test_conn')
            )
            assert len(reorg_results) == 1
            assert reorg_results[0].success

            # All data should be deleted due to overlap
            after_reorg_count = self.config.get_row_count(loader, test_table_name)
            assert after_reorg_count == 0

    @pytest.mark.integration
    def test_reorg_multi_network(self, loader, test_table_name, cleanup_tables):
        """Test that reorg only affects specified network"""
        if not self.config.supports_streaming:
            pytest.skip('Loader does not support streaming')
        if not self.config.supports_multi_network:
            pytest.skip('Loader does not support multi-network isolation')

        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

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
                    ranges_complete=True,
                ),
            )
            response_poly = ResponseBatch.data_batch(
                data=batch_poly,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='polygon', start=100, end=100, hash='0xbbb')],
                    ranges_complete=True,
                ),
            )

            # Load both batches via streaming API (with connection_name for state tracking)
            stream = [response_eth, response_poly]
            results = list(loader.load_stream_continuous(iter(stream), test_table_name, connection_name='test_conn'))
            assert len(results) == 2
            assert all(r.success for r in results)

            # Verify both networks' data exists
            initial_count = self.config.get_row_count(loader, test_table_name)
            assert initial_count == 2

            # Invalidate only ethereum network
            reorg_response = ResponseBatch.reorg_batch(
                invalidation_ranges=[BlockRange(network='ethereum', start=100, end=100)]
            )
            reorg_results = list(
                loader.load_stream_continuous(iter([reorg_response]), test_table_name, connection_name='test_conn')
            )
            assert len(reorg_results) == 1
            assert reorg_results[0].success

            # Should only delete ethereum data, polygon should remain
            after_reorg_count = self.config.get_row_count(loader, test_table_name)
            assert after_reorg_count == 1

    @pytest.mark.integration
    def test_microbatch_deduplication(self, loader, test_table_name, cleanup_tables):
        """
        Test that multiple RecordBatches within the same microbatch are all loaded,
        and deduplication only happens at microbatch boundaries when ranges_complete=True.

        This test verifies the fix for the critical bug where we were marking batches
        as processed after every RecordBatch instead of waiting for ranges_complete=True.
        """
        if not self.config.supports_streaming:
            pytest.skip('Loader does not support streaming')

        cleanup_tables.append(test_table_name)

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        with loader:
            # Create table first from the schema
            batch1_data = pa.RecordBatch.from_pydict({'id': [1, 2], 'value': [100, 200]})
            loader._create_table_from_schema(batch1_data.schema, test_table_name)

            # Simulate a microbatch sent as 3 RecordBatches with the same BlockRange
            # First RecordBatch of the microbatch (ranges_complete=False)
            response1 = ResponseBatch.data_batch(
                data=batch1_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],
                    ranges_complete=False,  # Not the last batch in this microbatch
                ),
            )

            # Second RecordBatch (same BlockRange, ranges_complete=False)
            batch2_data = pa.RecordBatch.from_pydict({'id': [3, 4], 'value': [300, 400]})
            response2 = ResponseBatch.data_batch(
                data=batch2_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],
                    ranges_complete=False,
                ),
            )

            # Third RecordBatch (same BlockRange, ranges_complete=True)
            batch3_data = pa.RecordBatch.from_pydict({'id': [5, 6], 'value': [500, 600]})
            response3 = ResponseBatch.data_batch(
                data=batch3_data,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],
                    ranges_complete=True,  # Last batch - safe to mark as processed
                ),
            )

            # Process the microbatch stream
            stream = [response1, response2, response3]
            results = list(
                loader.load_stream_continuous(iter(stream), test_table_name, connection_name='test_connection')
            )

            # CRITICAL: All 3 RecordBatches should be loaded successfully
            assert len(results) == 3, 'All RecordBatches within microbatch should be processed'
            assert all(r.success for r in results), 'All batches should succeed'
            assert results[0].rows_loaded == 2, 'First batch should load 2 rows'
            assert results[1].rows_loaded == 2, 'Second batch should load 2 rows (not skipped!)'
            assert results[2].rows_loaded == 2, 'Third batch should load 2 rows (not skipped!)'

            # Verify all 6 rows in table
            total_count = self.config.get_row_count(loader, test_table_name)
            assert total_count == 6, 'All 6 rows from 3 RecordBatches should be in the table'

            # Test duplicate detection - send the same microbatch again
            duplicate_batch = pa.RecordBatch.from_pydict({'id': [7, 8], 'value': [700, 800]})
            duplicate_response = ResponseBatch.data_batch(
                data=duplicate_batch,
                metadata=BatchMetadata(
                    ranges=[BlockRange(network='ethereum', start=100, end=110, hash='0xabc123')],
                    ranges_complete=True,
                ),
            )

            duplicate_results = list(
                loader.load_stream_continuous(
                    iter([duplicate_response]), test_table_name, connection_name='test_connection'
                )
            )

            # Duplicate should be skipped
            assert duplicate_results[0].rows_loaded == 0, 'Duplicate microbatch should be skipped'

            # Verify count hasn't changed
            final_count = self.config.get_row_count(loader, test_table_name)
            assert final_count == 6, 'No additional rows should be added after duplicate'
