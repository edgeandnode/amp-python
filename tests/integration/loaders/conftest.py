"""
Base test classes and configuration for loader integration tests.

This module provides abstract base classes that enable test generalization
across different loader implementations while maintaining full test coverage.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

import pytest

from src.amp.loaders.base import DataLoader


@pytest.fixture
def test_table_name():
    """Generate unique table name for each test"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return f'test_table_{timestamp}'


class LoaderTestConfig(ABC):
    """
    Configuration for a specific loader's tests.

    Each loader implementation must provide a concrete implementation
    that defines loader-specific query methods and capabilities.
    """

    # Required: Specify the loader class being tested
    loader_class: Type[DataLoader] = None

    # Required: Name of the pytest fixture that provides the loader config dict
    config_fixture_name: str = None

    # Loader capability flags
    supports_overwrite: bool = True
    supports_streaming: bool = True
    supports_multi_network: bool = True
    supports_null_values: bool = True

    @abstractmethod
    def get_row_count(self, loader: DataLoader, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            loader: The loader instance
            table_name: Name of the table

        Returns:
            Number of rows in the table
        """
        raise NotImplementedError

    @abstractmethod
    def query_rows(
        self, loader: DataLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query rows from a table.

        Args:
            loader: The loader instance
            table_name: Name of the table
            where: Optional WHERE clause (without 'WHERE' keyword)
            order_by: Optional ORDER BY clause (without 'ORDER BY' keyword)

        Returns:
            List of dictionaries representing rows
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup_table(self, loader: DataLoader, table_name: str) -> None:
        """
        Clean up/drop a table.

        Args:
            loader: The loader instance
            table_name: Name of the table to drop
        """
        raise NotImplementedError

    @abstractmethod
    def get_column_names(self, loader: DataLoader, table_name: str) -> List[str]:
        """
        Get column names for a table.

        Args:
            loader: The loader instance
            table_name: Name of the table

        Returns:
            List of column names
        """
        raise NotImplementedError


class LoaderTestBase:
    """
    Base class for all loader tests.

    Test classes should inherit from this and set the `config` class attribute
    to a LoaderTestConfig instance.

    Example:
        class TestPostgreSQLCore(BaseLoaderTests):
            config = PostgreSQLTestConfig()
    """

    # Override this in subclasses
    config: LoaderTestConfig = None

    @pytest.fixture
    def loader(self, request):
        """
        Create a loader instance from the config.

        This fixture dynamically retrieves the loader config from the
        fixture name specified in LoaderTestConfig.config_fixture_name.
        """
        if self.config is None:
            raise ValueError('Test class must define a config attribute')
        if self.config.loader_class is None:
            raise ValueError('LoaderTestConfig must define loader_class')
        if self.config.config_fixture_name is None:
            raise ValueError('LoaderTestConfig must define config_fixture_name')

        # Get the loader config from the specified fixture
        loader_config = request.getfixturevalue(self.config.config_fixture_name)

        # Create and return the loader instance
        return self.config.loader_class(loader_config)

    @pytest.fixture
    def cleanup_tables(self, request):
        """
        Cleanup fixture that drops tables after tests.

        Tests should append table names to this list, and they will be
        cleaned up automatically after the test completes.
        """
        tables_to_clean = []

        yield tables_to_clean

        # Cleanup after test
        if tables_to_clean and self.config:
            loader_config = request.getfixturevalue(self.config.config_fixture_name)
            loader = self.config.loader_class(loader_config)
            try:
                loader.connect()
                for table_name in tables_to_clean:
                    try:
                        self.config.cleanup_table(loader, table_name)
                    except Exception:
                        # Ignore cleanup errors
                        pass
                loader.disconnect()
            except Exception:
                # Ignore connection errors during cleanup
                pass
