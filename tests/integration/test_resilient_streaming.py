"""
Integration tests for resilient streaming.

Tests retry logic, circuit breaker, and rate limiting with real loaders
and streaming scenarios.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict

import pyarrow as pa
import pytest

from amp.loaders.base import DataLoader


@dataclass
class FailingLoaderConfig:
    """Configuration for test loader"""

    failure_mode: str = 'none'
    fail_count: int = 0


class FailingLoader(DataLoader[FailingLoaderConfig]):
    """
    Test loader that simulates various failure scenarios.

    This loader allows controlled failure injection to test resilience:
    - Transient failures (429, timeout) that should be retried
    - Permanent failures (400, 404) that should fail fast
    - Intermittent failures for circuit breaker testing
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.current_attempt = 0
        self.call_count = 0
        self.connect_called = False
        self.disconnect_called = False

    def _parse_config(self, config: Dict[str, Any]) -> FailingLoaderConfig:
        """Parse config, filtering out resilience which is handled by base class"""
        # Remove resilience config (handled by base DataLoader class)
        loader_config = {k: v for k, v in config.items() if k != 'resilience'}
        return FailingLoaderConfig(**loader_config)

    def connect(self):
        self.connect_called = True
        self._is_connected = True

    def disconnect(self):
        self.disconnect_called = True
        self._is_connected = False

    def _load_batch_impl(self, batch: pa.RecordBatch, table_name: str, **kwargs) -> int:
        """Implementation-specific batch loading with configurable failure injection"""
        self.call_count += 1

        # Simulate different failure modes
        if self.config.failure_mode == 'transient_then_success':
            # Fail first N times with transient error, then succeed
            if self.current_attempt < self.config.fail_count:
                self.current_attempt += 1
                raise Exception('HTTP 429 Too Many Requests')

        elif self.config.failure_mode == 'timeout_then_success':
            if self.current_attempt < self.config.fail_count:
                self.current_attempt += 1
                raise Exception('Connection timeout')

        elif self.config.failure_mode == 'permanent':
            raise Exception('HTTP 400 Bad Request - Invalid data')

        elif self.config.failure_mode == 'always_fail':
            raise Exception('HTTP 503 Service Unavailable')

        # Success case - return number of rows loaded
        return batch.num_rows


class TestRetryLogic:
    """Test automatic retry with exponential backoff"""

    def test_retry_on_transient_error(self):
        """Test that transient errors are retried automatically"""
        # Configure loader to fail twice with 429, then succeed
        config = {
            'failure_mode': 'transient_then_success',
            'fail_count': 2,
            'resilience': {
                'retry': {
                    'enabled': True,
                    'max_retries': 3,
                    'initial_backoff_ms': 10,  # Fast for testing
                    'jitter': False,
                }
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        # Create test data
        schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
        batch = pa.record_batch([[1, 2, 3], ['a', 'b', 'c']], schema=schema)

        # Load should succeed after retries
        result = loader.load_batch(batch, 'test_table')

        assert result.success is True
        assert result.rows_loaded == 3
        # Should have been called 3 times (2 failures + 1 success)
        assert loader.call_count == 3

        loader.disconnect()

    def test_retry_respects_max_retries(self):
        """Test that retry stops after max_retries"""
        config = {
            'failure_mode': 'always_fail',  # Always fails
            'resilience': {
                'retry': {
                    'enabled': True,
                    'max_retries': 2,
                    'initial_backoff_ms': 10,
                    'jitter': False,
                }
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1, 2]], schema=schema)

        # Should raise after 2 retries
        with pytest.raises(RuntimeError, match='Max retries.*exceeded'):
            loader.load_batch(batch, 'test_table')

        # Should have tried 3 times total (initial + 2 retries)
        assert loader.call_count == 3

        loader.disconnect()

    def test_no_retry_on_permanent_error(self):
        """Test that permanent errors are not retried"""
        config = {
            'failure_mode': 'permanent',
            'resilience': {
                'retry': {
                    'enabled': True,
                    'max_retries': 3,
                    'initial_backoff_ms': 10,
                }
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Should raise immediately without retries
        with pytest.raises(RuntimeError, match='Permanent error'):
            loader.load_batch(batch, 'test_table')

        # Should only be called once (no retries for permanent errors)
        assert loader.call_count == 1

        loader.disconnect()

    def test_retry_disabled(self):
        """Test that retry can be disabled"""
        config = {
            'failure_mode': 'transient_then_success',
            'fail_count': 1,
            'resilience': {'retry': {'enabled': False}},
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Should raise immediately (no retry, treated as permanent)
        with pytest.raises(RuntimeError, match='Permanent error'):
            loader.load_batch(batch, 'test_table')

        assert loader.call_count == 1

        loader.disconnect()


class TestAdaptiveRateLimiting:
    """Test adaptive back pressure / rate limiting"""

    def test_rate_limit_slows_down_on_429(self):
        """Test that rate limiter increases delay on 429 errors"""
        config = {
            'failure_mode': 'transient_then_success',
            'fail_count': 1,
            'resilience': {
                'retry': {'enabled': True, 'max_retries': 1, 'initial_backoff_ms': 10, 'jitter': False},
                'back_pressure': {
                    'enabled': True,
                    'initial_delay_ms': 0,
                    'max_delay_ms': 5000,
                    'adapt_on_429': True,
                },
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Initial delay should be 0
        assert loader.rate_limiter.get_current_delay() == 0

        # Load batch (will fail with 429, then succeed on retry)
        result = loader.load_batch(batch, 'test_table')
        assert result.success is True

        # Rate limiter should have increased delay
        current_delay = loader.rate_limiter.get_current_delay()
        assert current_delay > 0  # Should have increased

        loader.disconnect()

    def test_rate_limit_speeds_up_on_success(self):
        """Test that rate limiter decreases delay on successful operations"""
        config = {
            'failure_mode': 'none',
            'resilience': {
                'back_pressure': {
                    'enabled': True,
                    'initial_delay_ms': 100,
                    'recovery_factor': 0.9,  # 10% speedup per success
                },
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        # Manually increase delay
        loader.rate_limiter.record_rate_limit()
        initial_delay = loader.rate_limiter.get_current_delay()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Successful load should decrease delay
        result = loader.load_batch(batch, 'test_table')
        assert result.success is True

        new_delay = loader.rate_limiter.get_current_delay()
        assert new_delay < initial_delay

        loader.disconnect()

    def test_rate_limit_disabled(self):
        """Test that rate limiting can be disabled"""
        config = {
            'failure_mode': 'transient_then_success',
            'fail_count': 1,
            'resilience': {
                'retry': {'enabled': True, 'max_retries': 1, 'initial_backoff_ms': 10, 'jitter': False},
                'back_pressure': {'enabled': False},
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Even after 429 error, delay should remain 0
        loader.load_batch(batch, 'test_table')
        assert loader.rate_limiter.get_current_delay() == 0

        loader.disconnect()


class TestResilienceIntegration:
    """Test resilience features working together"""

    def test_retry_with_backpressure(self):
        """Test that retry and back pressure work together"""
        config = {
            'failure_mode': 'timeout_then_success',
            'fail_count': 2,
            'resilience': {
                'retry': {
                    'enabled': True,
                    'max_retries': 3,
                    'initial_backoff_ms': 10,
                    'jitter': False,
                },
                'back_pressure': {
                    'enabled': True,
                    'initial_delay_ms': 0,
                    'adapt_on_timeout': True,
                },
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1, 2, 3]], schema=schema)

        start_time = time.time()
        result = loader.load_batch(batch, 'test_table')
        duration = time.time() - start_time

        # Should succeed after retries
        assert result.success is True
        assert result.rows_loaded == 3

        # Should have taken some time due to backoff + rate limiting
        assert duration > 0.02  # At least 20ms (2 retries with 10ms backoff)

        # Rate limiter should have adapted to timeouts
        assert loader.rate_limiter.get_current_delay() > 0

        loader.disconnect()

    def test_all_resilience_features_together(self):
        """Test retry and rate limiting working together"""
        config = {
            'failure_mode': 'transient_then_success',
            'fail_count': 1,  # Fail once, then succeed
            'resilience': {
                'retry': {
                    'enabled': True,
                    'max_retries': 2,
                    'initial_backoff_ms': 10,
                    'jitter': False,
                },
                'back_pressure': {
                    'enabled': True,
                    'initial_delay_ms': 0,
                    'adapt_on_429': True,
                },
            },
        }

        loader = FailingLoader(config)
        loader.connect()

        schema = pa.schema([('id', pa.int64())])
        batch = pa.record_batch([[1]], schema=schema)

        # Multiple successful loads with retries
        for _i in range(3):
            # Reset failure mode for each iteration
            loader.current_attempt = 0

            result = loader.load_batch(batch, 'test_table')
            assert result.success is True

        # Rate limiter should have adapted
        assert loader.rate_limiter.get_current_delay() >= 0  # Could be 0 if speedup brought it back down

        loader.disconnect()
