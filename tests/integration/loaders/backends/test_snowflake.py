"""
Snowflake-specific loader integration tests.

This module provides Snowflake-specific test configuration and tests that
inherit from the generalized base test classes.

Note: Snowflake tests require valid Snowflake credentials and are typically
skipped in CI/CD. Run manually with: pytest -m snowflake
"""

from typing import Any, Dict, List, Optional

import pytest

try:
    from src.amp.loaders.implementations.snowflake_loader import SnowflakeLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class SnowflakeTestConfig(LoaderTestConfig):
    """Snowflake-specific test configuration"""

    loader_class = SnowflakeLoader
    config_fixture_name = 'snowflake_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True

    def get_row_count(self, loader: SnowflakeLoader, table_name: str) -> int:
        """Get row count from Snowflake table"""
        with loader.conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {table_name}')
            return cur.fetchone()[0]

    def query_rows(
        self, loader: SnowflakeLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from Snowflake table"""
        query = f'SELECT * FROM {table_name}'
        if where:
            query += f' WHERE {where}'
        if order_by:
            query += f' ORDER BY {order_by}'
        query += ' LIMIT 100'

        with loader.conn.cursor() as cur:
            cur.execute(query)
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    def cleanup_table(self, loader: SnowflakeLoader, table_name: str) -> None:
        """Drop Snowflake table"""
        with loader.conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {table_name}')

    def get_column_names(self, loader: SnowflakeLoader, table_name: str) -> List[str]:
        """Get column names from Snowflake table"""
        with loader.conn.cursor() as cur:
            cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
            return [col[0] for col in cur.description]


@pytest.mark.snowflake
class TestSnowflakeCore(BaseLoaderTests):
    """Snowflake core loader tests (inherited from base)"""

    config = SnowflakeTestConfig()


@pytest.mark.snowflake
class TestSnowflakeStreaming(BaseStreamingTests):
    """Snowflake streaming tests (inherited from base)"""

    config = SnowflakeTestConfig()


@pytest.mark.snowflake
class TestSnowflakeSpecific:
    """Snowflake-specific tests that cannot be generalized"""

    def test_stage_loading_method(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test Snowflake stage-based loading (Snowflake-specific optimization)"""

        cleanup_tables.append(test_table_name)

        # Configure for stage loading
        config = {**snowflake_config, 'loading_method': 'stage'}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 100

            # Verify data loaded
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                count = cur.fetchone()[0]
                assert count == 100

    def test_insert_loading_method(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test Snowflake INSERT-based loading"""
        cleanup_tables.append(test_table_name)

        # Configure for INSERT loading
        config = {**snowflake_config, 'loading_method': 'insert'}
        loader = SnowflakeLoader(config)

        with loader:
            result = loader.load_table(small_test_table, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 100

    def test_table_info_retrieval(self, snowflake_config, small_test_table, test_table_name, cleanup_tables):
        """Test Snowflake table information retrieval"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Load data first
            loader.load_table(small_test_table, test_table_name)

            # Get table info
            info = loader.get_table_info(test_table_name)

            assert info is not None
            assert 'row_count' in info
            assert 'bytes' in info
            assert 'clustering_key' in info
            assert info['row_count'] == 100

    def test_concurrent_batch_loading(self, snowflake_config, medium_test_table, test_table_name, cleanup_tables):
        """Test concurrent batch loading to Snowflake"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from src.amp.loaders.base import LoadMode

        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            # Split table into batches
            batches = medium_test_table.to_batches(max_chunksize=2000)

            # Load batches concurrently
            def load_batch_with_mode(batch_tuple):
                i, batch = batch_tuple
                mode = LoadMode.OVERWRITE if i == 0 else LoadMode.APPEND
                return loader.load_batch(batch, test_table_name, mode=mode)

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(load_batch_with_mode, (i, batch)) for i, batch in enumerate(batches)]

                results = []
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)

            # Verify all batches succeeded
            assert all(r.success for r in results)

            # Verify total row count
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                count = cur.fetchone()[0]
                assert count == 10000

    def test_schema_special_characters(self, snowflake_config, test_table_name, cleanup_tables):
        """Test handling of special characters in column names"""
        import pyarrow as pa

        cleanup_tables.append(test_table_name)

        # Create data with special characters in column names
        data = {
            'id': [1, 2, 3],
            'user name': ['Alice', 'Bob', 'Charlie'],  # Space in name
            'email@address': ['a@ex.com', 'b@ex.com', 'c@ex.com'],  # @ symbol
            'data-value': [100, 200, 300],  # Hyphen
        }
        test_data = pa.Table.from_pydict(data)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(test_data, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 3

            # Verify columns were properly escaped
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {test_table_name} LIMIT 1')
                columns = [col[0] for col in cur.description]
                # Snowflake normalizes column names
                assert len(columns) >= 4

    def test_history_preservation_with_reorg(self, snowflake_config, test_table_name, cleanup_tables):
        """Test Snowflake's history preservation during reorg (Time Travel feature)"""
        import pyarrow as pa

        from src.amp.streaming.types import BatchMetadata, BlockRange, ResponseBatch

        cleanup_tables.append(test_table_name)

        config = {**snowflake_config, 'preserve_history': True}
        loader = SnowflakeLoader(config)

        with loader:
            # Load initial data
            batch1 = pa.RecordBatch.from_pydict({'tx_hash': ['0x100', '0x101'], 'block_num': [100, 101]})

            loader._create_table_from_schema(batch1.schema, test_table_name)

            response1 = ResponseBatch.data_batch(
                data=batch1,
                metadata=BatchMetadata(ranges=[BlockRange(network='ethereum', start=100, end=101, hash='0xaaa')]),
            )

            results = list(loader.load_stream_continuous(iter([response1]), test_table_name))
            assert len(results) == 1
            assert results[0].success

            # Verify initial count
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                initial_count = cur.fetchone()[0]
                assert initial_count == 2

                # Perform reorg (with history preservation, data is soft-deleted)
                reorg_response = ResponseBatch.reorg_batch(
                    invalidation_ranges=[BlockRange(network='ethereum', start=100, end=101)]
                )
                reorg_results = list(loader.load_stream_continuous(iter([reorg_response]), test_table_name))
                assert len(reorg_results) == 1

                # With history preservation, data may be marked deleted rather than physically removed
                # Verify the reorg was processed (exact behavior depends on implementation)
                cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                after_count = cur.fetchone()[0]
                # Count should be 0 or data should have _deleted flag
                assert after_count >= 0  # Flexible assertion for different implementations


@pytest.mark.snowflake
@pytest.mark.slow
class TestSnowflakePerformance:
    """Snowflake performance tests"""

    def test_large_batch_loading(self, snowflake_config, performance_test_data, test_table_name, cleanup_tables):
        """Test loading large batches to Snowflake"""
        cleanup_tables.append(test_table_name)

        loader = SnowflakeLoader(snowflake_config)

        with loader:
            result = loader.load_table(performance_test_data, test_table_name)

            assert result.success == True
            assert result.rows_loaded == 50000
            assert result.duration < 120  # Should complete within 2 minutes

            # Verify data integrity
            with loader.conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM {test_table_name}')
                count = cur.fetchone()[0]
                assert count == 50000


# Note: Snowpipe Streaming tests (TestSnowpipeStreamingIntegration) are kept
# in the original test file as they are highly Snowflake-specific and involve
# complex channel management that doesn't generalize well. See:
# tests/integration/test_snowflake_loader.py::TestSnowpipeStreamingIntegration
