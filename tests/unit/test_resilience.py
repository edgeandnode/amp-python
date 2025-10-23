"""
Unit tests for resilience primitives.

Tests error classification, backoff calculation, circuit breaker state machine,
and adaptive rate limiting without external dependencies.
"""

import time
from unittest.mock import patch

import pytest

from amp.streaming.resilience import (
    AdaptiveRateLimiter,
    BackPressureConfig,
    ErrorClassifier,
    ExponentialBackoff,
    RetryConfig,
)


class TestRetryConfig:
    """Test RetryConfig dataclass validation and defaults"""

    def test_default_values(self):
        config = RetryConfig()
        assert config.enabled is True
        assert config.max_retries == 5  # Production-grade default
        assert config.initial_backoff_ms == 2000  # Start with 2s delay
        assert config.max_backoff_ms == 120000  # Cap at 2 minutes
        assert config.backoff_multiplier == 2.0
        assert config.jitter is True

    def test_custom_values(self):
        config = RetryConfig(
            enabled=False, max_retries=5, initial_backoff_ms=500, max_backoff_ms=30000, backoff_multiplier=1.5, jitter=False
        )
        assert config.enabled is False
        assert config.max_retries == 5
        assert config.initial_backoff_ms == 500
        assert config.max_backoff_ms == 30000
        assert config.backoff_multiplier == 1.5
        assert config.jitter is False


class TestBackPressureConfig:
    """Test BackPressureConfig dataclass"""

    def test_default_values(self):
        config = BackPressureConfig()
        assert config.enabled is True
        assert config.initial_delay_ms == 0
        assert config.max_delay_ms == 5000
        assert config.adapt_on_429 is True
        assert config.adapt_on_timeout is True
        assert config.recovery_factor == 0.9

    def test_custom_values(self):
        config = BackPressureConfig(
            enabled=False, initial_delay_ms=100, max_delay_ms=10000, adapt_on_429=False, adapt_on_timeout=False, recovery_factor=0.8
        )
        assert config.enabled is False
        assert config.initial_delay_ms == 100
        assert config.max_delay_ms == 10000
        assert config.adapt_on_429 is False
        assert config.adapt_on_timeout is False
        assert config.recovery_factor == 0.8


class TestErrorClassifier:
    """Test error classification logic"""

    def test_transient_errors(self):
        """Test that transient error patterns are correctly identified"""
        transient_errors = [
            'Connection timeout occurred',
            'HTTP 429 Too Many Requests',
            'HTTP 503 Service Unavailable',
            'HTTP 504 Gateway Timeout',
            'Connection reset by peer',
            'Temporary failure in name resolution',
            'Service unavailable, please retry',
            'Too many requests',
            'Rate limit exceeded',
            'Request throttled',
            'Connection error',
            'Broken pipe',
            'Connection refused',
            'Operation timed out',
        ]

        for error in transient_errors:
            assert ErrorClassifier.is_transient(error), f'Expected transient: {error}'

    def test_permanent_errors(self):
        """Test that permanent errors are not classified as transient"""
        permanent_errors = [
            'HTTP 400 Bad Request',
            'HTTP 401 Unauthorized',
            'HTTP 403 Forbidden',
            'HTTP 404 Not Found',
            'Invalid credentials',
            'Schema validation failed',
            'Table does not exist',
            'SQL syntax error',
            'Column not found',
        ]

        for error in permanent_errors:
            assert not ErrorClassifier.is_transient(error), f'Expected permanent: {error}'

    def test_case_insensitive(self):
        """Test that classification is case-insensitive"""
        assert ErrorClassifier.is_transient('TIMEOUT')
        assert ErrorClassifier.is_transient('Timeout')
        assert ErrorClassifier.is_transient('timeout')
        assert ErrorClassifier.is_transient('TimeOut')

    def test_empty_error(self):
        """Test that empty errors are not classified as transient"""
        assert not ErrorClassifier.is_transient('')
        assert not ErrorClassifier.is_transient(None)


class TestExponentialBackoff:
    """Test exponential backoff calculation logic"""

    def test_basic_exponential_growth(self):
        """Test that backoff grows exponentially without jitter"""
        config = RetryConfig(initial_backoff_ms=100, backoff_multiplier=2.0, max_backoff_ms=10000, jitter=False)
        backoff = ExponentialBackoff(config)

        # First retry: 100ms
        delay1 = backoff.next_delay()
        assert delay1 == 0.1  # 100ms in seconds

        # Second retry: 200ms
        delay2 = backoff.next_delay()
        assert delay2 == 0.2  # 200ms in seconds

        # Third retry: 400ms
        delay3 = backoff.next_delay()
        assert delay3 == 0.4  # 400ms in seconds

    def test_max_backoff_cap(self):
        """Test that backoff is capped at max_backoff_ms"""
        config = RetryConfig(initial_backoff_ms=1000, backoff_multiplier=10.0, max_backoff_ms=5000, jitter=False, max_retries=5)
        backoff = ExponentialBackoff(config)

        # First retry: 1000ms
        delay1 = backoff.next_delay()
        assert delay1 == 1.0

        # Second retry: 10000ms, capped at 5000ms
        delay2 = backoff.next_delay()
        assert delay2 == 5.0  # Capped at max

    def test_jitter_randomization(self):
        """Test that jitter adds randomness to backoff"""
        config = RetryConfig(initial_backoff_ms=1000, backoff_multiplier=2.0, jitter=True, max_retries=10)

        # Run multiple times to verify randomness
        delays = []
        for _ in range(10):
            backoff = ExponentialBackoff(config)
            delay = backoff.next_delay()
            delays.append(delay)

        # With jitter, delays should vary between 50-150% of base (0.5s - 1.5s)
        assert all(0.5 <= d <= 1.5 for d in delays), f'Jittered delays out of range: {delays}'
        # Should have some variation (not all the same)
        assert len(set(delays)) > 1, 'Expected variation in jittered delays'

    def test_max_retries_limit(self):
        """Test that backoff returns None after max_retries"""
        config = RetryConfig(initial_backoff_ms=100, max_retries=3, jitter=False)
        backoff = ExponentialBackoff(config)

        # 3 successful delays
        assert backoff.next_delay() is not None
        assert backoff.next_delay() is not None
        assert backoff.next_delay() is not None

        # 4th attempt should fail
        assert backoff.next_delay() is None

    def test_reset(self):
        """Test that reset() resets the backoff state"""
        config = RetryConfig(initial_backoff_ms=100, jitter=False)
        backoff = ExponentialBackoff(config)

        # First attempt
        delay1 = backoff.next_delay()
        assert delay1 == 0.1

        # Second attempt
        delay2 = backoff.next_delay()
        assert delay2 == 0.2

        # Reset and try again
        backoff.reset()
        delay3 = backoff.next_delay()
        assert delay3 == 0.1  # Back to initial delay


class TestAdaptiveRateLimiter:
    """Test adaptive rate limiting logic"""

    def test_initial_delay(self):
        """Test that initial delay is applied correctly"""
        config = BackPressureConfig(initial_delay_ms=100)
        limiter = AdaptiveRateLimiter(config)

        assert limiter.get_current_delay() == 100

    def test_success_speeds_up(self):
        """Test that successes gradually reduce delay"""
        config = BackPressureConfig(initial_delay_ms=100, recovery_factor=0.9)
        limiter = AdaptiveRateLimiter(config)

        # Increase delay first
        limiter.record_rate_limit()  # 100 * 2 + 1000 = 1200ms

        initial_delay = limiter.get_current_delay()
        assert initial_delay == 1200

        # Success should reduce by 10%
        limiter.record_success()
        assert limiter.get_current_delay() == int(1200 * 0.9)  # 1080ms

    def test_rate_limit_slows_down(self):
        """Test that 429 responses significantly increase delay"""
        config = BackPressureConfig(initial_delay_ms=100, max_delay_ms=10000)
        limiter = AdaptiveRateLimiter(config)

        limiter.record_rate_limit()
        # 100 * 2 + 1000 = 1200ms
        assert limiter.get_current_delay() == 1200

        limiter.record_rate_limit()
        # 1200 * 2 + 1000 = 3400ms
        assert limiter.get_current_delay() == 3400

    def test_timeout_slows_down_moderately(self):
        """Test that timeouts increase delay moderately"""
        config = BackPressureConfig(initial_delay_ms=100, max_delay_ms=10000)
        limiter = AdaptiveRateLimiter(config)

        limiter.record_timeout()
        # 100 * 1.5 + 500 = 650ms
        assert limiter.get_current_delay() == 650

    def test_max_delay_cap(self):
        """Test that delay is capped at max_delay_ms"""
        config = BackPressureConfig(initial_delay_ms=1000, max_delay_ms=5000)
        limiter = AdaptiveRateLimiter(config)

        # Record multiple rate limits
        for _ in range(10):
            limiter.record_rate_limit()

        # Should be capped at max
        assert limiter.get_current_delay() == 5000

    def test_delay_can_reach_zero(self):
        """Test that delay can decrease all the way to zero"""
        config = BackPressureConfig(initial_delay_ms=1000, recovery_factor=0.5)
        limiter = AdaptiveRateLimiter(config)

        # Start at initial delay
        assert limiter.get_current_delay() == 1000

        # Record many successes - should decrease to zero
        for _ in range(20):
            limiter.record_success()

        # Should reach zero (not floored at initial_delay_ms)
        assert limiter.get_current_delay() == 0

    def test_disabled_rate_limiter(self):
        """Test that disabled rate limiter doesn't apply delays"""
        config = BackPressureConfig(enabled=False)
        limiter = AdaptiveRateLimiter(config)

        start = time.time()
        limiter.wait()
        duration = time.time() - start

        # Should not wait
        assert duration < 0.01  # Less than 10ms

    def test_wait_applies_delay(self):
        """Test that wait() actually delays execution"""
        config = BackPressureConfig(initial_delay_ms=50, enabled=True)
        limiter = AdaptiveRateLimiter(config)

        start = time.time()
        limiter.wait()
        duration = time.time() - start

        # Should wait approximately 50ms
        assert duration >= 0.04  # At least 40ms (some tolerance)
        assert duration < 0.1  # But not too long

    def test_adapt_on_429_disabled(self):
        """Test that adapt_on_429=False prevents rate limit adaptation"""
        config = BackPressureConfig(initial_delay_ms=100, adapt_on_429=False)
        limiter = AdaptiveRateLimiter(config)

        limiter.record_rate_limit()
        # Should not change
        assert limiter.get_current_delay() == 100

    def test_adapt_on_timeout_disabled(self):
        """Test that adapt_on_timeout=False prevents timeout adaptation"""
        config = BackPressureConfig(initial_delay_ms=100, adapt_on_timeout=False)
        limiter = AdaptiveRateLimiter(config)

        limiter.record_timeout()
        # Should not change
        assert limiter.get_current_delay() == 100
