"""
Redis-specific loader integration tests.

This module provides Redis-specific test configuration and tests that
inherit from the generalized base test classes.
"""

import json
from typing import Any, Dict, List, Optional

import pytest

try:
    import redis

    from src.amp.loaders.implementations.redis_loader import RedisLoader
    from tests.integration.loaders.conftest import LoaderTestConfig
    from tests.integration.loaders.test_base_loader import BaseLoaderTests
    from tests.integration.loaders.test_base_streaming import BaseStreamingTests
except ImportError:
    pytest.skip('amp modules not available', allow_module_level=True)


class RedisTestConfig(LoaderTestConfig):
    """Redis-specific test configuration"""

    loader_class = RedisLoader
    config_fixture_name = 'redis_test_config'

    supports_overwrite = True
    supports_streaming = True
    supports_multi_network = True
    supports_null_values = True

    def get_row_count(self, loader: RedisLoader, table_name: str) -> int:
        """Get row count from Redis based on data structure"""
        data_structure = getattr(loader.config, 'data_structure', 'hash')

        if data_structure == 'hash':
            # Count hash keys matching pattern
            pattern = f'{table_name}:*'
            count = 0
            for _ in loader.redis_client.scan_iter(match=pattern, count=1000):
                count += 1
            return count
        elif data_structure == 'string':
            # Count string keys matching pattern
            pattern = f'{table_name}:*'
            count = 0
            for _ in loader.redis_client.scan_iter(match=pattern, count=1000):
                count += 1
            return count
        elif data_structure == 'stream':
            # Get stream length
            try:
                return loader.redis_client.xlen(table_name)
            except Exception:
                return 0
        else:
            # For other structures, scan for keys
            pattern = f'{table_name}:*'
            count = 0
            for _ in loader.redis_client.scan_iter(match=pattern, count=1000):
                count += 1
            return count

    def query_rows(
        self, loader: RedisLoader, table_name: str, where: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query rows from Redis - limited functionality due to Redis architecture"""
        # Redis doesn't support SQL-like queries, so this is a simplified implementation
        data_structure = getattr(loader.config, 'data_structure', 'hash')
        rows = []

        if data_structure == 'hash':
            pattern = f'{table_name}:*'
            for key in loader.redis_client.scan_iter(match=pattern, count=100):
                data = loader.redis_client.hgetall(key)
                row = {k.decode(): v.decode() if isinstance(v, bytes) else v for k, v in data.items()}
                rows.append(row)
        elif data_structure == 'stream':
            # Read from stream
            messages = loader.redis_client.xrange(table_name, count=100)
            for msg_id, data in messages:
                row = {k.decode(): v.decode() if isinstance(v, bytes) else v for k, v in data.items()}
                row['_stream_id'] = msg_id.decode()
                rows.append(row)

        return rows[:100]  # Limit to 100 rows

    def cleanup_table(self, loader: RedisLoader, table_name: str) -> None:
        """Drop Redis keys for table"""
        # Delete all keys matching the table pattern
        patterns = [f'{table_name}:*', table_name]

        for pattern in patterns:
            for key in loader.redis_client.scan_iter(match=pattern, count=1000):
                loader.redis_client.delete(key)

    def get_column_names(self, loader: RedisLoader, table_name: str) -> List[str]:
        """Get column names from Redis - return fields from first record"""
        data_structure = getattr(loader.config, 'data_structure', 'hash')

        if data_structure == 'hash':
            pattern = f'{table_name}:*'
            for key in loader.redis_client.scan_iter(match=pattern, count=1):
                data = loader.redis_client.hgetall(key)
                return [k.decode() if isinstance(k, bytes) else k for k in data.keys()]
        elif data_structure == 'stream':
            messages = loader.redis_client.xrange(table_name, count=1)
            if messages:
                _, data = messages[0]
                return [k.decode() if isinstance(k, bytes) else k for k in data.keys()]

        return []


@pytest.fixture
def cleanup_redis(redis_test_config):
    """Cleanup Redis data after tests"""
    keys_to_clean = []
    patterns_to_clean = []

    yield (keys_to_clean, patterns_to_clean)

    # Cleanup
    try:
        r = redis.Redis(
            host=redis_test_config['host'],
            port=redis_test_config['port'],
            db=redis_test_config['db'],
            password=redis_test_config['password'],
        )

        # Delete specific keys
        for key in keys_to_clean:
            r.delete(key)

        # Delete keys matching patterns
        for pattern in patterns_to_clean:
            for key in r.scan_iter(match=pattern, count=1000):
                r.delete(key)

        r.close()
    except Exception:
        pass


@pytest.mark.redis
class TestRedisCore(BaseLoaderTests):
    """Redis core loader tests (inherited from base)"""

    config = RedisTestConfig()


@pytest.mark.redis
class TestRedisStreaming(BaseStreamingTests):
    """Redis streaming tests (inherited from base)"""

    config = RedisTestConfig()


@pytest.mark.redis
class TestRedisSpecific:
    """Redis-specific tests that cannot be generalized"""

    def test_hash_storage(self, redis_test_config, small_test_data, cleanup_redis):
        """Test Redis hash data structure storage"""

        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_hash'
        patterns_to_clean.append(f'{table_name}:*')

        config = {**redis_test_config, 'data_structure': 'hash', 'key_field': 'id'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify data is stored as hashes
            pattern = f'{table_name}:*'
            keys = list(loader.redis_client.scan_iter(match=pattern, count=100))
            assert len(keys) == 5

            # Verify hash structure
            first_key = keys[0]
            data = loader.redis_client.hgetall(first_key)
            assert len(data) > 0

    def test_string_storage(self, redis_test_config, small_test_data, cleanup_redis):
        """Test Redis string data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_string'
        patterns_to_clean.append(f'{table_name}:*')

        config = {**redis_test_config, 'data_structure': 'string', 'key_field': 'id'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify data is stored as strings
            pattern = f'{table_name}:*'
            keys = list(loader.redis_client.scan_iter(match=pattern, count=100))
            assert len(keys) == 5

            # Verify string structure (should be JSON)
            first_key = keys[0]
            data = loader.redis_client.get(first_key)
            assert data is not None
            # Should be JSON-encoded
            json.loads(data)

    def test_stream_storage(self, redis_test_config, small_test_data, cleanup_redis):
        """Test Redis stream data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_stream'
        keys_to_clean.append(table_name)

        config = {**redis_test_config, 'data_structure': 'stream'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 5

            # Verify data is in stream
            stream_len = loader.redis_client.xlen(table_name)
            assert stream_len == 5

    def test_set_storage(self, redis_test_config, small_test_data, cleanup_redis):
        """Test Redis set data structure storage"""
        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_set'
        patterns_to_clean.append(f'{table_name}:*')

        config = {**redis_test_config, 'data_structure': 'set', 'key_field': 'id'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True
            assert result.rows_loaded == 5

    def test_ttl_functionality(self, redis_test_config, small_test_data, cleanup_redis):
        """Test TTL (time-to-live) functionality"""
        import time

        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_ttl'
        patterns_to_clean.append(f'{table_name}:*')

        config = {**redis_test_config, 'data_structure': 'hash', 'key_field': 'id', 'ttl': 2}  # 2 second TTL
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(small_test_data, table_name)
            assert result.success == True

            # Verify data exists
            pattern = f'{table_name}:*'
            keys_before = list(loader.redis_client.scan_iter(match=pattern, count=100))
            assert len(keys_before) == 5

            # Verify TTL is set
            ttl = loader.redis_client.ttl(keys_before[0])
            assert ttl > 0 and ttl <= 2

            # Wait for TTL to expire
            time.sleep(3)

            # Verify data has expired
            keys_after = list(loader.redis_client.scan_iter(match=pattern, count=100))
            assert len(keys_after) == 0

    def test_key_pattern_generation(self, redis_test_config, cleanup_redis):
        """Test custom key pattern generation"""
        import pyarrow as pa

        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_key_pattern'
        patterns_to_clean.append(f'{table_name}:*')

        data = {'user_id': ['user1', 'user2', 'user3'], 'score': [100, 200, 300], 'level': [1, 2, 3]}
        test_data = pa.Table.from_pydict(data)

        config = {
            **redis_test_config,
            'data_structure': 'hash',
            'key_field': 'user_id',
            'key_pattern': '{table}:user:{key_value}',
        }
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(test_data, table_name)
            assert result.success == True

            # Verify custom key pattern
            pattern = f'{table_name}:user:*'
            keys = list(loader.redis_client.scan_iter(match=pattern, count=100))
            assert len(keys) == 3

            # Verify key format
            key_str = keys[0].decode() if isinstance(keys[0], bytes) else keys[0]
            assert key_str.startswith(f'{table_name}:user:')

    def test_data_structure_comparison(self, redis_test_config, comprehensive_test_data, cleanup_redis):
        """Test performance comparison between different data structures"""
        import time

        keys_to_clean, patterns_to_clean = cleanup_redis

        structures = ['hash', 'string', 'stream']
        results = {}

        for structure in structures:
            table_name = f'test_perf_{structure}'
            patterns_to_clean.append(f'{table_name}:*')
            keys_to_clean.append(table_name)

            config = {**redis_test_config, 'data_structure': structure}
            if structure in ['hash', 'string']:
                config['key_field'] = 'id'

            loader = RedisLoader(config)

            with loader:
                start_time = time.time()
                result = loader.load_table(comprehensive_test_data, table_name)
                duration = time.time() - start_time

                results[structure] = {
                    'success': result.success,
                    'duration': duration,
                    'rows_loaded': result.rows_loaded,
                }

        # Verify all structures work
        for _structure, data in results.items():
            assert data['success'] == True
            assert data['rows_loaded'] == 1000


@pytest.mark.redis
@pytest.mark.slow
class TestRedisPerformance:
    """Redis performance tests"""

    def test_large_data_loading(self, redis_test_config, cleanup_redis):
        """Test loading large datasets to Redis"""
        import pyarrow as pa

        keys_to_clean, patterns_to_clean = cleanup_redis
        table_name = 'test_large'
        patterns_to_clean.append(f'{table_name}:*')

        # Create large dataset
        large_data = {
            'id': list(range(10000)),
            'value': [i * 0.123 for i in range(10000)],
            'category': [f'cat_{i % 100}' for i in range(10000)],
        }
        large_table = pa.Table.from_pydict(large_data)

        config = {**redis_test_config, 'data_structure': 'hash', 'key_field': 'id'}
        loader = RedisLoader(config)

        with loader:
            result = loader.load_table(large_table, table_name)

            assert result.success == True
            assert result.rows_loaded == 10000
            assert result.duration < 60  # Should complete within 60 seconds
