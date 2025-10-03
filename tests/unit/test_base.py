# tests/unit/test_base.py

"""
Unit tests for base classes and utilities.
Updated for amp project structure.
"""

import ast
import importlib
import inspect
from pathlib import Path
from typing import Dict, List

import pytest

try:
    from src.amp.loaders.base import DataLoader, LoadConfig, LoadMode, LoadResult
    from src.amp.streaming.types import BlockRange
except ImportError:
    # Skip tests if modules not available
    pytest.skip('amp modules not available', allow_module_level=True)

from tests.fixtures.mock_clients import MockDataLoader


@pytest.mark.unit
class TestLoadResult:
    """Test LoadResult dataclass"""

    def test_success_result_string_representation(self):
        """Test string representation of successful result"""
        result = LoadResult(
            rows_loaded=1000,
            duration=2.5,
            ops_per_second=400.0,
            table_name='test_table',
            loader_type='postgresql',
            success=True,
        )

        result_str = str(result)
        assert '✅' in result_str
        assert '1000 rows' in result_str
        assert '2.50s' in result_str
        assert 'test_table' in result_str

    def test_failure_result_string_representation(self):
        """Test string representation of failed result"""
        result = LoadResult(
            rows_loaded=0,
            duration=1.0,
            ops_per_second=0.0,
            table_name='test_table',
            loader_type='postgresql',
            success=False,
            error='Connection failed',
        )

        result_str = str(result)
        assert '❌' in result_str
        assert 'Connection failed' in result_str
        assert 'test_table' in result_str

    def test_reorg_result_string_representation(self):
        """Test string representation of reorg LoadResult"""
        invalidation_ranges = [
            BlockRange(network='ethereum', start=100, end=110),
            BlockRange(network='polygon', start=200, end=205),
        ]

        result = LoadResult(
            rows_loaded=0,
            duration=0.5,
            ops_per_second=0,
            table_name='blocks',
            loader_type='postgresql',
            success=True,
            is_reorg=True,
            invalidation_ranges=invalidation_ranges,
        )

        result_str = str(result)
        assert '🔄 Reorg detected' in result_str
        assert '2 ranges invalidated' in result_str


@pytest.mark.unit
class TestLoadConfig:
    """Test LoadConfig dataclass"""

    def test_default_values(self):
        """Test default configuration values"""
        config = LoadConfig()

        assert config.batch_size == 10000
        assert config.mode == LoadMode.APPEND
        assert config.create_table == True
        assert config.schema_evolution == False
        assert config.max_retries == 3
        assert config.retry_delay == 1.0

    def test_custom_values(self):
        """Test custom configuration values"""
        config = LoadConfig(batch_size=5000, mode=LoadMode.OVERWRITE, create_table=False, max_retries=5)

        assert config.batch_size == 5000
        assert config.mode == LoadMode.OVERWRITE
        assert config.create_table == False
        assert config.max_retries == 5


@pytest.mark.unit
class TestMockDataLoader:
    """Test MockDataLoader functionality"""

    def test_successful_batch_loading(self, small_test_table):
        """Test successful batch loading"""
        loader = MockDataLoader({'test': 'config'})
        batch = small_test_table.to_batches()[0]

        with loader:
            result = loader.load_batch(batch, 'test_table')

        assert result.success
        assert result.rows_loaded == batch.num_rows
        assert result.table_name == 'test_table'
        assert result.loader_type == 'mock'
        assert len(loader.load_calls) == 1

    def test_successful_table_loading(self, small_test_table):
        """Test successful table loading"""
        loader = MockDataLoader({'test': 'config'})

        with loader:
            result = loader.load_table(small_test_table, 'test_table')

        assert result.success
        assert result.rows_loaded == small_test_table.num_rows
        assert len(loader.load_calls) == 1

    def test_failure_simulation(self, small_test_table):
        """Test failure simulation"""
        loader = MockDataLoader({'test': 'config'})
        loader.should_fail = True
        loader.fail_message = 'Simulated failure'

        with loader:
            result = loader.load_table(small_test_table, 'test_table')

        assert not result.success
        assert result.error == 'Simulated failure'
        assert result.rows_loaded == 0


@pytest.mark.unit
class TestLoaderImplementations:
    """Test that all loader implementations follow the required patterns"""

    def _get_loader_classes(self) -> Dict[str, type]:
        """Get all loader implementation classes"""
        loaders = {}
        implementations_dir = Path('src/amp/loaders/implementations')

        for py_file in implementations_dir.glob('*.py'):
            if py_file.name.startswith('_') or py_file.name == '__init__.py':
                continue

            module_name = f'src.amp.loaders.implementations.{py_file.stem}'
            try:
                module = importlib.import_module(module_name)
                for name in dir(module):
                    obj = getattr(module, name)
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, DataLoader)
                        and obj != DataLoader
                        and name.endswith('Loader')
                    ):
                        loaders[name] = obj
            except ImportError:
                # Skip if dependencies not available
                continue

        return loaders

    def _get_method_definitions(self, loader_class: type, method_name: str) -> List[str]:
        """Get all definitions of a method in a class"""
        source_file = inspect.getfile(loader_class)
        with open(source_file, 'r') as f:
            source = f.read()

        tree = ast.parse(source)
        method_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == loader_class.__name__:
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == method_name:
                        method_defs.append(f'Line {item.lineno}')

        return method_defs

    def _verify_method_implementation(self, loader_name: str, loader_class: type, method_name: str) -> None:
        """Verify that a method is actually implemented, not just inherited as a stub"""
        method = getattr(loader_class, method_name)

        # First try to check the source code
        try:
            source = inspect.getsource(method)

            # Check if method is inherited from base class (not defined in this specific class)
            # If 'class <LoaderClassName>' is NOT in source, it means method comes from base class
            if f'class {loader_class}' not in source:
                # Method is inherited, check if it's a stub
                if method_name == '_handle_reorg':
                    # For _handle_reorg, check if it just raises NotImplementedError
                    if 'raise NotImplementedError' in source:
                        pytest.fail(
                            f'{loader_name} does not implement {method_name}() - it inherits the '
                            f'NotImplementedError from base class. Each loader must implement '
                            f'this method appropriately for its storage backend.'
                        )
            elif (
                'pass' in source
                and len([line for line in source.split('\n') if line.strip() and not line.strip().startswith('#')]) <= 2
            ):
                # Method is just 'pass'
                pytest.fail(f"{loader_name}.{method_name} is just 'pass' - needs implementation")

        except (OSError, TypeError):
            # Can't get source, try runtime approach for _handle_reorg
            if method_name == '_handle_reorg':
                try:
                    # Create a dummy instance to test the method
                    test_instance = loader_class({'test': 'config'})
                    test_instance._handle_reorg([], 'test_table')
                    # If we get here, method didn't raise NotImplementedError - it's implemented
                except NotImplementedError:
                    # Method raises NotImplementedError - not implemented
                    pytest.fail(
                        f'{loader_name} does not implement {method_name}() - it raises NotImplementedError. '
                        f'Each loader must implement this method.'
                    )
                except Exception:
                    # Some other error occurred during execution, assume it's implemented
                    pass

    def test_all_loaders_implement_required_methods(self):
        """Test that all loader implementations properly implement required methods (not just inherit stubs)"""
        required_methods = ['connect', 'disconnect', '_load_batch_impl', '_create_table_from_schema', '_handle_reorg']

        loaders = self._get_loader_classes()

        assert len(loaders) > 0, 'No loader classes found'

        for loader_name, loader_class in loaders.items():
            for method_name in required_methods:
                assert hasattr(loader_class, method_name), f'{loader_name} missing required method: {method_name}'

                # Check that the method is actually implemented (not just inherited stub)
                method = getattr(loader_class, method_name)
                assert method is not None, f'{loader_name}.{method_name} is None'

                # Verify the method is actually implemented in this class, not just a stub
                self._verify_method_implementation(loader_name, loader_class, method_name)

    def test_no_duplicate_method_definitions(self):
        """Test that no loader has duplicate method definitions"""
        critical_methods = ['_create_table_from_schema', '_load_batch_impl', 'connect', 'disconnect']

        loaders = self._get_loader_classes()

        for loader_name, loader_class in loaders.items():
            for method_name in critical_methods:
                definitions = self._get_method_definitions(loader_class, method_name)
                assert len(definitions) <= 1, f'{loader_name} has duplicate {method_name} definitions at: {definitions}'
