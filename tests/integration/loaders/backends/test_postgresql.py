"""
PostgreSQL-specific loader integration tests.

This module provides PostgreSQL-specific test configuration and tests that
inherit from the generalized base test classes.
"""

import time
from typing import Any, Dict, List, Optional

import pytest

try:
    from src.amp.loaders.implementations.postgresql_loader import PostgreSQLLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class PostgreSQLTestConfig(LoaderTestConfig):
    """PostgreSQL-specific test configuration"""

    loader_class = PostgreSQLLoader
    config_fixture_name = 'postgresql_test_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True

    def get_row_count(self, loader: PostgreSQLLoader, table_name: str) -> int:
        """Get row count from PostgreSQL table"""
        conn = loader.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM {table_name}')
                return cur.fetchone()[0]
        finally:
            loader.pool.putconn(conn)

    def query_rows(
        self, loader: PostgreSQLLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from PostgreSQL table"""
        conn = loader.pool.getconn()
        try:
            with conn.cursor() as cur:
                # Get column names first
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                )
                columns = [row[0] for row in cur.fetchall()]

                # Build query
                query = f'SELECT * FROM {table_name}'
                if where:
                    query += f' WHERE {where}'
                if order_by:
                    query += f' ORDER BY {order_by}'

                cur.execute(query)
                rows = cur.fetchall()

                # Convert to list of dicts
                return [dict(zip(columns, row, strict=False)) for row in rows]
        finally:
            loader.pool.putconn(conn)

    def cleanup_table(self, loader: PostgreSQLLoader, table_name: str) -> None:
        """Drop PostgreSQL table"""
        conn = loader.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE')
                conn.commit()
        finally:
            loader.pool.putconn(conn)

    def get_column_names(self, loader: PostgreSQLLoader, table_name: str) -> List[str]:
        """Get column names from PostgreSQL table"""
        conn = loader.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            loader.pool.putconn(conn)


@pytest.mark.postgresql
class TestPostgreSQLCore(BaseLoaderTests):
    """PostgreSQL core loader tests (inherited from base)"""

    config = PostgreSQLTestConfig()


@pytest.mark.postgresql
class TestPostgreSQLStreaming(BaseStreamingTests):
    """PostgreSQL streaming tests (inherited from base)"""

    config = PostgreSQLTestConfig()


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


@pytest.mark.postgresql
class TestPostgreSQLSpecific:
    """PostgreSQL-specific tests that cannot be generalized"""

    def test_connection_pooling(self, postgresql_test_config, small_test_data, test_table_name, cleanup_tables):
        """Test PostgreSQL connection pooling behavior"""
        from src.amp.loaders.base import LoadMode

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

    def test_binary_data_handling(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test binary data handling with INSERT fallback"""
        import pyarrow as pa

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

    def test_null_value_handling_detailed(
        self, postgresql_test_config, null_test_data, test_table_name, cleanup_tables
    ):
        """Test comprehensive null value handling across all PostgreSQL data types"""
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
                    cur.execute(f'SELECT COUNT(*) FROM {test_table_name} WHERE timestamp_field IS NULL')
                    timestamp_nulls = cur.fetchone()[0]
                    assert timestamp_nulls == 4

                    # Check json field nulls
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


@pytest.mark.postgresql
@pytest.mark.slow
class TestPostgreSQLPerformance:
    """PostgreSQL performance tests"""

    def test_large_data_loading(self, postgresql_test_config, test_table_name, cleanup_tables):
        """Test loading large datasets"""
        from datetime import datetime

        import pyarrow as pa

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
