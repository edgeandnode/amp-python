"""
Generalized core loader tests that work across all loader implementations.

These tests use the LoaderTestConfig abstraction to run identical test logic
across different storage backends (PostgreSQL, Redis, Snowflake, etc.).
"""

import pytest

from src.amp.loaders.base import LoadMode
from tests.integration.loaders.conftest import LoaderTestBase


class BaseLoaderTests(LoaderTestBase):
    """
    Base test class with core loader functionality tests.

    All loaders should inherit from this class and provide a LoaderTestConfig.

    Example:
        class TestPostgreSQLCore(BaseLoaderTests):
            config = PostgreSQLTestConfig()
    """

    @pytest.mark.integration
    def test_connection(self, loader):
        """Test basic connection and disconnection to the storage backend"""
        # Test connection
        loader.connect()
        assert loader._is_connected == True

        # Test disconnection
        loader.disconnect()
        assert loader._is_connected == False

    @pytest.mark.integration
    def test_context_manager(self, loader, small_test_data, test_table_name, cleanup_tables):
        """Test context manager functionality"""
        cleanup_tables.append(test_table_name)

        with loader:
            assert loader._is_connected == True

            result = loader.load_table(small_test_data, test_table_name)
            assert result.success == True

        # Should be disconnected after context
        assert loader._is_connected == False

    @pytest.mark.integration
    def test_batch_loading(self, loader, medium_test_table, test_table_name, cleanup_tables):
        """Test batch loading functionality with sequential batches"""
        cleanup_tables.append(test_table_name)

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
            total_rows = self.config.get_row_count(loader, test_table_name)
            assert total_rows == 10000

    @pytest.mark.integration
    def test_append_mode(self, loader, small_test_data, test_table_name, cleanup_tables):
        """Test append mode functionality"""
        cleanup_tables.append(test_table_name)

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
            total_rows = self.config.get_row_count(loader, test_table_name)
            assert total_rows == 10  # 5 + 5

    @pytest.mark.integration
    def test_overwrite_mode(self, loader, small_test_data, test_table_name, cleanup_tables):
        """Test overwrite mode functionality"""
        if not self.config.supports_overwrite:
            pytest.skip('Loader does not support overwrite mode')

        cleanup_tables.append(test_table_name)

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
            total_rows = self.config.get_row_count(loader, test_table_name)
            assert total_rows == 3

    @pytest.mark.integration
    def test_null_handling(self, loader, null_test_data, test_table_name, cleanup_tables):
        """Test null value handling across all data types"""
        if not self.config.supports_null_values:
            pytest.skip('Loader does not support null values')

        cleanup_tables.append(test_table_name)

        with loader:
            result = loader.load_table(null_test_data, test_table_name)
            assert result.success == True
            assert result.rows_loaded == 10

            # Verify data was loaded (basic sanity check)
            # Specific null value verification is loader-specific and tested in backend tests
            total_rows = self.config.get_row_count(loader, test_table_name)
            assert total_rows == 10

    @pytest.mark.integration
    def test_error_handling(self, loader, small_test_data):
        """Test error handling scenarios"""
        with loader:
            # Test loading to non-existent table without create_table
            result = loader.load_table(small_test_data, 'non_existent_table', create_table=False)

            assert result.success == False
            assert result.error is not None
            assert result.rows_loaded == 0
