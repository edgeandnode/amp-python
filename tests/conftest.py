# tests/conftest.py
"""
Shared pytest configuration and fixtures for the data loader test suite.
"""

import logging
import os
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

logging.basicConfig(level=logging.INFO)

# Control whether to use testcontainers
USE_TESTCONTAINERS = os.getenv('USE_TESTCONTAINERS', 'true').lower() == 'true'

# Disable Ryuk if not explicitly enabled (solves Docker connectivity issues)
if 'TESTCONTAINERS_RYUK_DISABLED' not in os.environ:
    os.environ['TESTCONTAINERS_RYUK_DISABLED'] = 'true'

# Set Docker host for Colima if not already set
if 'DOCKER_HOST' not in os.environ:
    colima_socket = Path.home() / '.colima' / 'default' / 'docker.sock'
    if colima_socket.exists():
        os.environ['DOCKER_HOST'] = f'unix://{colima_socket}'

# Import testcontainers conditionally
if USE_TESTCONTAINERS:
    try:
        from testcontainers.postgres import PostgresContainer
        from testcontainers.redis import RedisContainer

        TESTCONTAINERS_AVAILABLE = True
    except ImportError:
        TESTCONTAINERS_AVAILABLE = False
        logging.warning('Testcontainers not available. Falling back to manual configuration.')
else:
    TESTCONTAINERS_AVAILABLE = False


# Shared configuration fixtures
@pytest.fixture(scope='session')
def postgresql_config():
    """PostgreSQL configuration from environment or defaults"""
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'database': os.getenv('POSTGRES_DB', 'test_amp'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'max_connections': 10,
        'batch_size': 10000,
    }


@pytest.fixture(scope='session')
def redis_config():
    """Redis configuration from environment or defaults"""
    return {
        'host': os.getenv('REDIS_HOST', 'localhost'),
        'port': int(os.getenv('REDIS_PORT', '6379')),
        'db': int(os.getenv('REDIS_DB', '1')),
        'password': os.getenv('REDIS_PASSWORD', 'mypassword'),
        'max_connections': 10,
        'batch_size': 100,
        'pipeline_size': 500,
    }


@pytest.fixture(scope='session')
def snowflake_config():
    """Snowflake configuration from environment or defaults"""
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT', 'test_account'),
        'user': os.getenv('SNOWFLAKE_USER', 'test_user'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'test_warehouse'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'test_database'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        'loading_method': 'stage',  # Default to stage loading for existing tests
    }

    # Add optional parameters if they exist
    if os.getenv('SNOWFLAKE_PASSWORD'):
        config['password'] = os.getenv('SNOWFLAKE_PASSWORD')
    if os.getenv('SNOWFLAKE_PRIVATE_KEY'):
        config['private_key'] = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    if os.getenv('SNOWFLAKE_ROLE'):
        config['role'] = os.getenv('SNOWFLAKE_ROLE')
    if os.getenv('SNOWFLAKE_AUTHENTICATOR'):
        config['authenticator'] = os.getenv('SNOWFLAKE_AUTHENTICATOR')

    return config


@pytest.fixture(scope='session')
def test_config():
    """Test configuration that can be overridden by environment variables"""
    return {
        'postgresql': {
            'host': os.getenv('TEST_POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('TEST_POSTGRES_PORT', 5432)),
            'database': os.getenv('TEST_POSTGRES_DB', 'test_db'),
            'user': os.getenv('TEST_POSTGRES_USER', 'test_user'),
            'password': os.getenv('TEST_POSTGRES_PASSWORD', 'test_pass'),
        },
        'redis': {
            'host': os.getenv('TEST_REDIS_HOST', 'localhost'),
            'port': int(os.getenv('TEST_REDIS_PORT', 6379)),
            'db': int(os.getenv('TEST_REDIS_DB', 0)),
        },
        'delta_lake': {
            'base_path': os.getenv('TEST_DELTA_BASE_PATH', None),  # Will use temp if None
        },
    }


# Testcontainers fixtures
@pytest.fixture(scope='session')
def postgres_container():
    """PostgreSQL container for integration tests"""
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip('Testcontainers not available')

    import time

    from testcontainers.core.waiting_utils import wait_for_logs

    container = PostgresContainer(image='postgres:13', username='test_user', password='test_pass', dbname='test_db')
    container.start()

    # Wait for PostgreSQL to be ready using log message
    wait_for_logs(container, 'database system is ready to accept connections', timeout=30)

    # PostgreSQL logs "ready" twice - wait a bit more to ensure fully ready
    time.sleep(2)

    yield container

    container.stop()


@pytest.fixture(scope='session')
def redis_container():
    """Redis container for integration tests"""
    if not TESTCONTAINERS_AVAILABLE:
        pytest.skip('Testcontainers not available')

    from testcontainers.core.waiting_utils import wait_for_logs

    container = RedisContainer(image='redis:7-alpine')
    container.start()

    # Wait for Redis to be ready using log message
    wait_for_logs(container, 'Ready to accept connections', timeout=30)

    yield container

    container.stop()


@pytest.fixture(scope='session')
def postgresql_test_config(request):
    """PostgreSQL configuration from testcontainer or environment"""
    if TESTCONTAINERS_AVAILABLE and USE_TESTCONTAINERS:
        # Get the postgres_container fixture
        postgres_container = request.getfixturevalue('postgres_container')
        return {
            'host': postgres_container.get_container_host_ip(),
            'port': postgres_container.get_exposed_port(5432),
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'max_connections': 10,
            'batch_size': 10000,
        }
    else:
        # Fall back to manual config from environment
        return request.getfixturevalue('postgresql_config')


@pytest.fixture(scope='session')
def redis_test_config(request):
    """Redis configuration from testcontainer or environment"""
    if TESTCONTAINERS_AVAILABLE and USE_TESTCONTAINERS:
        # Get the redis_container fixture
        redis_container = request.getfixturevalue('redis_container')
        return {
            'host': redis_container.get_container_host_ip(),
            'port': redis_container.get_exposed_port(6379),
            'db': 0,
            'password': None,  # Default Redis container has no password
            'max_connections': 10,
            'batch_size': 100,
            'pipeline_size': 500,
        }
    else:
        # Fall back to manual config from environment
        return request.getfixturevalue('redis_config')


@pytest.fixture
def redis_streaming_config(redis_test_config):
    """Redis config for streaming tests with blockchain data (uses tx_hash instead of id)"""
    return {
        **redis_test_config,
        'key_pattern': '{table}:{tx_hash}',  # Use tx_hash from blockchain data
        'data_structure': 'hash',
    }


@pytest.fixture(scope='session')
def delta_test_env():
    """Create Delta Lake test environment for the session"""
    # Create temporary directory for Delta Lake tests
    temp_dir = tempfile.mkdtemp(prefix='delta_test_')

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def delta_basic_config(delta_test_env):
    """Basic Delta Lake configuration for testing"""
    return {
        'table_path': str(Path(delta_test_env) / 'basic_table'),
        'partition_by': ['year', 'month'],
        'optimize_after_write': True,
        'vacuum_after_write': False,
        'schema_evolution': True,
        'merge_schema': True,
        'storage_options': {},
    }


@pytest.fixture
def delta_partitioned_config(delta_test_env):
    """Partitioned Delta Lake configuration for testing"""
    return {
        'table_path': str(Path(delta_test_env) / 'partitioned_table'),
        'partition_by': ['year', 'month', 'day'],
        'optimize_after_write': True,
        'vacuum_after_write': True,
        'schema_evolution': True,
        'merge_schema': True,
        'storage_options': {},
    }


@pytest.fixture
def delta_streaming_config(delta_test_env):
    """Delta Lake configuration for streaming tests (no partitioning)"""
    return {
        'table_path': str(Path(delta_test_env) / 'streaming_table'),
        'partition_by': [],  # No partitioning for streaming tests
        'optimize_after_write': True,
        'vacuum_after_write': False,
        'schema_evolution': True,
        'merge_schema': True,
        'storage_options': {},
    }


@pytest.fixture
def delta_temp_config(delta_test_env):
    """Temporary Delta Lake configuration with unique path"""
    unique_path = str(Path(delta_test_env) / f'temp_table_{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}')
    return {
        'table_path': unique_path,
        'partition_by': ['year', 'month'],
        'optimize_after_write': False,
        'vacuum_after_write': False,
        'schema_evolution': True,
        'merge_schema': True,
        'storage_options': {},
    }


@pytest.fixture(scope='session')
def iceberg_test_env():
    """Create Iceberg test environment for the session"""
    # Create temporary directory for Iceberg tests
    temp_dir = tempfile.mkdtemp(prefix='iceberg_test_')

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def iceberg_basic_config(iceberg_test_env):
    """Basic Iceberg configuration for testing"""
    return {
        'catalog_config': {
            'type': 'sql',
            'uri': f'sqlite:///{iceberg_test_env}/catalog.db',
            'warehouse': f'file://{iceberg_test_env}/warehouse',
        },
        'namespace': 'test_data',
        'create_namespace': True,
        'create_table': True,
        'schema_evolution': True,
        'batch_size': 10000,
    }


@pytest.fixture
def iceberg_streaming_config(iceberg_test_env):
    """Iceberg configuration for streaming tests (no partitioning)"""
    return {
        'catalog_config': {
            'type': 'sql',
            'uri': f'sqlite:///{iceberg_test_env}/streaming_catalog.db',
            'warehouse': f'file://{iceberg_test_env}/streaming_warehouse',
        },
        'namespace': 'test_data',
        'create_namespace': True,
        'create_table': True,
        'schema_evolution': True,
        'batch_size': 10000,
        'partition_spec': [],  # No partitioning for streaming tests
    }


@pytest.fixture
def lmdb_test_env():
    """Create LMDB test environment for the session"""
    temp_dir = tempfile.mkdtemp(prefix='lmdb_test_')

    yield temp_dir

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def lmdb_perf_config(lmdb_test_env):
    """LMDB configuration optimized for performance testing"""
    return {
        'db_path': str(Path(lmdb_test_env) / 'perf_test.lmdb'),
        'map_size': 1024 * 1024**2,  # 1GB for performance tests
        'transaction_size': 10000,
        'writemap': True,
        'sync': False,  # Faster for tests, less durable
        'readahead': False,
        'max_readers': 16,
        'create_if_missing': True,
    }


@pytest.fixture
def small_test_table():
    """Small Arrow table for quick tests"""
    data = {
        'id': range(100),
        'name': [f'user_{i}' for i in range(100)],
        'value': [i * 0.1 for i in range(100)],
        'active': [i % 2 == 0 for i in range(100)],
        'created_at': [datetime.now() - timedelta(hours=i) for i in range(100)],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def medium_test_table():
    """Medium Arrow table for integration tests"""
    data = {
        'id': range(10000),
        'name': [f'user_{i}' for i in range(10000)],
        'category': [['A', 'B', 'C'][i % 3] for i in range(10000)],
        'score': [i * 0.01 for i in range(10000)],
        'active': [i % 2 == 0 for i in range(10000)],
        'created_date': [date.today() - timedelta(days=i % 365) for i in range(10000)],
        'updated_at': [datetime.now() - timedelta(hours=i) for i in range(10000)],
        'year': [2024 if i < 8000 else 2023 for i in range(10000)],
        'month': [(i // 100) % 12 + 1 for i in range(10000)],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def comprehensive_test_data():
    """Comprehensive test data for Delta Lake testing"""
    base_date = datetime(2024, 1, 1)

    data = {
        'id': list(range(1000)),
        'user_id': [f'user_{i % 100}' for i in range(1000)],
        'transaction_amount': [round((i * 12.34) % 1000, 2) for i in range(1000)],
        'category': [['electronics', 'clothing', 'books', 'food', 'travel'][i % 5] for i in range(1000)],
        'timestamp': [(base_date + timedelta(days=i // 50, hours=i % 24)).isoformat() for i in range(1000)],
        'year': [2024 if i < 800 else 2023 for i in range(1000)],
        'month': [(i // 80) % 12 + 1 for i in range(1000)],
        'day': [(i // 30) % 28 + 1 for i in range(1000)],
        'is_weekend': [i % 7 in [0, 6] for i in range(1000)],
        'metadata': [
            f'{{"session_id": "session_{i}", "device": "{["mobile", "desktop", "tablet"][i % 3]}"}}'
            for i in range(1000)
        ],
        'score': [i * 0.123 for i in range(1000)],
        'active': [i % 2 == 0 for i in range(1000)],
    }

    return pa.Table.from_pydict(data)


@pytest.fixture
def small_test_data():
    """Small test data for quick tests"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['a', 'b', 'c', 'd', 'e'],
        'value': [10.1, 20.2, 30.3, 40.4, 50.5],
        'year': [2024, 2024, 2024, 2024, 2024],
        'month': [1, 1, 1, 1, 1],
        'day': [1, 2, 3, 4, 5],
        'active': [True, False, True, False, True],
    }

    return pa.Table.from_pydict(data)


@pytest.fixture
def null_test_data():
    """Test data specifically designed for null value handling across all loaders"""
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'text_field': ['a', 'b', None, 'd', 'e', None, 'g', 'h', None, 'j'],
        'int_field': [1, None, 3, 4, None, 6, 7, None, 9, 10],
        'float_field': [1.1, 2.2, None, 4.4, 5.5, None, 7.7, 8.8, None, 10.0],
        'bool_field': [True, False, None, True, False, None, True, False, None, True],
        'timestamp_field': [datetime.now() - timedelta(days=i) if i % 3 != 0 else None for i in range(10)],
        'json_field': [f'{{"key": "value_{i}"}}' if i % 4 != 0 else None for i in range(10)],
        # Partitioning fields for Delta Lake
        'year': [2024] * 10,
        'month': [1] * 10,
        'day': [i % 28 + 1 for i in range(10)],
    }
    return pa.Table.from_pydict(data)


def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line('markers', 'unit: Unit tests (fast, no external dependencies)')
    config.addinivalue_line('markers', 'integration: Integration tests (require databases)')
    config.addinivalue_line('markers', 'performance: Performance and benchmark tests')
    config.addinivalue_line('markers', 'postgresql: Tests requiring PostgreSQL')
    config.addinivalue_line('markers', 'redis: Tests requiring Redis')
    config.addinivalue_line('markers', 'delta_lake: Tests requiring Delta Lake')
    config.addinivalue_line('markers', 'iceberg: Tests requiring Apache Iceberg')
    config.addinivalue_line('markers', 'snowflake: Tests requiring Snowflake')
    config.addinivalue_line('markers', 'lmdb: Tests requiring LMDB')
    config.addinivalue_line('markers', 'slow: Slow tests (> 30 seconds)')


# Utility fixtures for mocking
@pytest.fixture
def mock_flight_client():
    """Mock Flight SQL client for unit tests"""
    from unittest.mock import Mock

    mock_client = Mock()
    mock_client.conn = Mock()
    return mock_client


@pytest.fixture
def mock_connection_manager():
    """Mock connection manager for testing"""
    from unittest.mock import Mock

    mock_manager = Mock()
    mock_manager.connections = {}

    def mock_get_connection_info(name):
        if name == 'test_connection':
            return {
                'loader': 'postgresql',
                'config': {'host': 'localhost', 'database': 'test_db', 'user': 'test_user', 'password': 'test_pass'},
            }
        raise ValueError(f"Connection '{name}' not found")

    mock_manager.get_connection_info = mock_get_connection_info
    return mock_manager


# Performance testing utilities
@pytest.fixture
def performance_test_data():
    """Large dataset for performance testing"""
    size = int(os.getenv('PERF_TEST_SIZE', '50000'))
    data = {
        'id': list(range(size)),
        'user_id': [f'user_{i % 1000}' for i in range(size)],
        'value': [i * 0.123 for i in range(size)],
        'category': [f'cat_{i % 100}' for i in range(size)],
        'description': [f'Description for item {i}' for i in range(size)],
        'score': [i * 10 for i in range(size)],
        'active': [i % 2 == 0 for i in range(size)],
        'timestamp': [time.time() + i for i in range(size)],
        'year': [2024 if i < size * 0.8 else 2023 for i in range(size)],
        'month': [(i // 100) % 12 + 1 for i in range(size)],
    }
    return pa.Table.from_pydict(data)


@pytest.fixture
def memory_monitor():
    """Monitor memory usage during tests"""
    try:
        import psutil

        process = psutil.Process()
        initial_memory = process.memory_info().rss

        yield {'initial_mb': initial_memory / 1024 / 1024}

        final_memory = process.memory_info().rss
        return {
            'initial_mb': initial_memory / 1024 / 1024,
            'final_mb': final_memory / 1024 / 1024,
            'delta_mb': (final_memory - initial_memory) / 1024 / 1024,
        }
    except ImportError:
        yield {'initial_mb': 0}


# Environment setup helpers
@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment before each test"""
    # Set environment variables for testing
    os.environ['TESTING'] = '1'

    yield

    # Cleanup after test
    if 'TESTING' in os.environ:
        del os.environ['TESTING']
