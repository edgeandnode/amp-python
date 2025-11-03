"""
Resilience primitives for production-grade streaming.

Provides retry logic, circuit breaker pattern, and adaptive back pressure
to handle transient failures, rate limiting, and service outages gracefully.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff."""

    enabled: bool = True
    max_retries: int = 5  # More generous default for production durability
    initial_backoff_ms: int = 2000  # Start with 2s delay
    max_backoff_ms: int = 120000  # Cap at 2 minutes
    backoff_multiplier: float = 2.0
    jitter: bool = True  # Add randomness to prevent thundering herd


@dataclass
class BackPressureConfig:
    """Configuration for adaptive back pressure / rate limiting."""

    enabled: bool = True
    initial_delay_ms: int = 0
    max_delay_ms: int = 5000
    adapt_on_429: bool = True  # Slow down on rate limit responses
    adapt_on_timeout: bool = True  # Slow down on timeouts
    recovery_factor: float = 0.9  # How fast to speed up after success (10% speedup)


class ErrorClassifier:
    """Classify errors as transient (retryable) or permanent (fatal)."""

    TRANSIENT_PATTERNS = [
        'timeout',
        '429',
        '503',
        '504',
        'connection reset',
        'temporary failure',
        'service unavailable',
        'too many requests',
        'rate limit',
        'throttle',
        'connection error',
        'broken pipe',
        'connection refused',
        'timed out',
    ]

    @staticmethod
    def is_transient(error: str) -> bool:
        """
        Determine if an error is transient and worth retrying.

        Args:
            error: Error message or exception string

        Returns:
            True if error appears transient, False if permanent
        """
        if not error:
            return False

        error_lower = error.lower()
        return any(pattern in error_lower for pattern in ErrorClassifier.TRANSIENT_PATTERNS)


class ExponentialBackoff:
    """
    Calculate exponential backoff delays with optional jitter.

    Jitter helps prevent thundering herd when many clients retry simultaneously.
    """

    def __init__(self, config: RetryConfig):
        self.config = config
        self.attempt = 0

    def next_delay(self) -> Optional[float]:
        """
        Calculate next backoff delay in seconds.

        Returns:
            Delay in seconds, or None if max retries exceeded
        """
        if self.attempt >= self.config.max_retries:
            return None

        # Exponential backoff: initial * (multiplier ^ attempt)
        delay_ms = min(
            self.config.initial_backoff_ms * (self.config.backoff_multiplier**self.attempt),
            self.config.max_backoff_ms,
        )

        # Add jitter: randomize to 50-150% of calculated delay
        if self.config.jitter:
            delay_ms *= 0.5 + random.random()

        self.attempt += 1
        return delay_ms / 1000.0

    def reset(self):
        """Reset backoff state for new operation."""
        self.attempt = 0


class AdaptiveRateLimiter:
    """
    Adaptive rate limiting that adjusts delay based on error responses.

    Slows down when seeing rate limits (429) or timeouts.
    Speeds up gradually when operations succeed.
    """

    def __init__(self, config: BackPressureConfig):
        self.config = config
        self.current_delay_ms = config.initial_delay_ms
        self._lock = threading.Lock()

    def wait(self):
        """Wait before next request (applies current delay)."""
        if not self.config.enabled:
            return

        delay_ms = self.current_delay_ms
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    def record_success(self):
        """Speed up gradually after a successful operation."""
        if not self.config.enabled:
            return

        with self._lock:
            # Speed up by recovery_factor (e.g., 10% faster per success)
            # Can decrease all the way to zero - only delay when actually needed
            self.current_delay_ms = max(0, self.current_delay_ms * self.config.recovery_factor)

    def record_rate_limit(self):
        """Slow down significantly after rate limit response (429)."""
        if not self.config.enabled or not self.config.adapt_on_429:
            return

        with self._lock:
            # Double the delay + 1 second penalty
            self.current_delay_ms = min(self.current_delay_ms * 2 + 1000, self.config.max_delay_ms)

            logger.warning(
                f'Rate limit detected (429). Adaptive back pressure increased delay to {self.current_delay_ms}ms.'
            )

    def record_timeout(self):
        """Slow down moderately after timeout."""
        if not self.config.enabled or not self.config.adapt_on_timeout:
            return

        with self._lock:
            # 1.5x the delay + 500ms penalty
            self.current_delay_ms = min(self.current_delay_ms * 1.5 + 500, self.config.max_delay_ms)

            logger.info(f'Timeout detected. Adaptive back pressure increased delay to {self.current_delay_ms}ms.')

    def get_current_delay(self) -> int:
        """Get current delay in milliseconds (for monitoring)."""
        return int(self.current_delay_ms)
