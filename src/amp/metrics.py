"""
Metrics instrumentation for amp data processing.

This module provides observability metrics using Prometheus client.
Metrics can be exported via HTTP endpoint or push gateway.

Usage:
    from amp.metrics import get_metrics, MetricsConfig

    # Get the global metrics instance
    metrics = get_metrics()

    # Record processing metrics
    metrics.records_processed.labels(loader='postgresql', table='users').inc(1000)
    metrics.processing_latency.labels(loader='postgresql', operation='load_batch').observe(0.5)

    # Start HTTP server to expose metrics
    from amp.metrics import start_metrics_server
    start_metrics_server(port=8000)
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, Optional

# Optional prometheus_client import
# Custom histogram buckets for different pipeline phases
# Fetch phase - network latency, can be slow for large queries
FETCH_BUCKETS = (0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)  # 100ms to 2min

# Transform phase - CPU-bound, typically fast
TRANSFORM_BUCKETS = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5)  # 0.1ms to 500ms

# Write phase - I/O bound, varies by target
WRITE_BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)  # 1ms to 5s

try:
    from prometheus_client import (
        REGISTRY,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        Info,
        generate_latest,
        start_http_server,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Provide stub types for type hints
    Counter = Any  # type: ignore
    Gauge = Any  # type: ignore
    Histogram = Any  # type: ignore
    Info = Any  # type: ignore
    CollectorRegistry = Any  # type: ignore
    REGISTRY = None  # type: ignore


@dataclass
class MetricsConfig:
    """Configuration for metrics collection.

    Attributes:
        enabled: Whether metrics collection is enabled
        namespace: Prefix for all metric names (default: 'amp')
        subsystem: Optional subsystem name for grouping metrics
        default_labels: Labels applied to all metrics
        histogram_buckets: Custom histogram buckets for latency metrics
    """

    enabled: bool = True
    namespace: str = 'amp'
    subsystem: str = ''
    default_labels: Dict[str, str] = field(default_factory=dict)
    histogram_buckets: tuple = (
        0.001,  # 1ms
        0.005,  # 5ms
        0.01,  # 10ms
        0.025,  # 25ms
        0.05,  # 50ms
        0.1,  # 100ms
        0.25,  # 250ms
        0.5,  # 500ms
        1.0,  # 1s
        2.5,  # 2.5s
        5.0,  # 5s
        10.0,  # 10s
    )


class NullMetric:
    """No-op metric that silently ignores all operations.

    Used when prometheus_client is not available or metrics are disabled.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def labels(self, *args: Any, **kwargs: Any) -> 'NullMetric':
        return self

    def inc(self, amount: float = 1) -> None:
        pass

    def dec(self, amount: float = 1) -> None:
        pass

    def set(self, value: float) -> None:
        pass

    def observe(self, value: float) -> None:
        pass

    def info(self, val: Dict[str, str]) -> None:
        pass

    def time(self) -> 'NullTimer':
        return NullTimer()

    @contextmanager
    def track_inprogress(self) -> Iterator[None]:
        yield


class NullTimer:
    """No-op timer context manager."""

    def __enter__(self) -> 'NullTimer':
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class AmpMetrics:
    """Central metrics registry for amp data processing.

    Provides Prometheus metrics for monitoring data loading operations:
    - Records processed (counter)
    - Processing latency (histogram)
    - Error rates (counter by type)
    - Batch sizes (histogram)
    - Active connections (gauge)
    - Queue depths (gauge)

    Thread-safe singleton implementation.
    """

    _instance: Optional['AmpMetrics'] = None
    _lock = threading.Lock()

    def __new__(cls, config: Optional[MetricsConfig] = None) -> 'AmpMetrics':
        """Singleton pattern with lazy initialization."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self, config: Optional[MetricsConfig] = None) -> None:
        """Initialize metrics.

        Args:
            config: Optional metrics configuration
        """
        if self._initialized:
            return

        self._config = config or MetricsConfig()
        self._setup_metrics()
        self._initialized = True

    def _setup_metrics(self) -> None:
        """Set up all Prometheus metrics."""
        if not PROMETHEUS_AVAILABLE or not self._config.enabled:
            self._setup_null_metrics()
            return

        ns = self._config.namespace
        ss = self._config.subsystem

        # Records processed counter
        self.records_processed: Counter = self._get_or_create_metric(
            Counter,
            name='records_processed_total',
            documentation='Total number of records processed',
            labelnames=['loader', 'table', 'connection'],
            namespace=ns,
            subsystem=ss,
        )

        # Processing latency histogram
        self.processing_latency: Histogram = self._get_or_create_metric(
            Histogram,
            name='processing_latency_seconds',
            documentation='Time spent processing data',
            labelnames=['loader', 'operation'],
            namespace=ns,
            subsystem=ss,
            buckets=self._config.histogram_buckets,
        )

        # Error counter by type
        self.errors: Counter = self._get_or_create_metric(
            Counter,
            name='errors_total',
            documentation='Total number of errors',
            labelnames=['loader', 'error_type', 'table'],
            namespace=ns,
            subsystem=ss,
        )

        # Batch sizes histogram
        self.batch_sizes: Histogram = self._get_or_create_metric(
            Histogram,
            name='batch_size_records',
            documentation='Number of records per batch',
            labelnames=['loader', 'table'],
            namespace=ns,
            subsystem=ss,
            buckets=(10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000),
        )

        # Active connections gauge
        self.active_connections: Gauge = self._get_or_create_metric(
            Gauge,
            name='active_connections',
            documentation='Number of active connections',
            labelnames=['loader', 'target'],
            namespace=ns,
            subsystem=ss,
        )

        # Queue depth gauge
        self.queue_depth: Gauge = self._get_or_create_metric(
            Gauge,
            name='queue_depth',
            documentation='Number of items in processing queue',
            labelnames=['queue_name'],
            namespace=ns,
            subsystem=ss,
        )

        # Bytes processed counter
        self.bytes_processed: Counter = self._get_or_create_metric(
            Counter,
            name='bytes_processed_total',
            documentation='Total bytes processed',
            labelnames=['loader', 'table'],
            namespace=ns,
            subsystem=ss,
        )

        # Reorg events counter
        self.reorg_events: Counter = self._get_or_create_metric(
            Counter,
            name='reorg_events_total',
            documentation='Total blockchain reorganization events',
            labelnames=['loader', 'network', 'table'],
            namespace=ns,
            subsystem=ss,
        )

        # Retry attempts counter
        self.retry_attempts: Counter = self._get_or_create_metric(
            Counter,
            name='retry_attempts_total',
            documentation='Total retry attempts',
            labelnames=['loader', 'operation', 'reason'],
            namespace=ns,
            subsystem=ss,
        )

        # Build info (static metadata)
        self.build_info: Info = self._get_or_create_metric(
            Info,
            name='build_info',
            documentation='Build information',
            namespace=ns,
            subsystem=ss,
        )

        # Flight SQL metrics
        self.flight_fetch_latency: Histogram = self._get_or_create_metric(
            Histogram,
            name='flight_fetch_latency_seconds',
            documentation='Time spent in Flight SQL operations',
            labelnames=['operation'],  # get_info, do_get, read_chunk
            namespace=ns,
            subsystem=ss,
            buckets=FETCH_BUCKETS,
        )

        self.flight_bytes_received: Counter = self._get_or_create_metric(
            Counter,
            name='flight_bytes_received_total',
            documentation='Total bytes received from Flight SQL',
            labelnames=['query_type'],
            namespace=ns,
            subsystem=ss,
        )

        self.flight_batches_received: Counter = self._get_or_create_metric(
            Counter,
            name='flight_batches_received_total',
            documentation='Total Arrow batches received from Flight SQL',
            labelnames=['query_type'],
            namespace=ns,
            subsystem=ss,
        )

        # Streaming worker metrics
        self.streaming_workers_active: Gauge = self._get_or_create_metric(
            Gauge,
            name='streaming_workers_active',
            documentation='Number of currently active streaming workers',
            labelnames=['executor_id'],
            namespace=ns,
            subsystem=ss,
        )

        self.streaming_workers_total: Gauge = self._get_or_create_metric(
            Gauge,
            name='streaming_workers_total',
            documentation='Total number of streaming workers configured',
            labelnames=['executor_id'],
            namespace=ns,
            subsystem=ss,
        )

        self.streaming_batch_wait: Histogram = self._get_or_create_metric(
            Histogram,
            name='streaming_batch_wait_seconds',
            documentation='Time spent waiting for batches in streaming',
            labelnames=['partition_id'],
            namespace=ns,
            subsystem=ss,
            buckets=FETCH_BUCKETS,
        )

        self.streaming_worker_processing: Histogram = self._get_or_create_metric(
            Histogram,
            name='streaming_worker_processing_seconds',
            documentation='Time spent processing batches in streaming workers',
            labelnames=['partition_id'],
            namespace=ns,
            subsystem=ss,
            buckets=TRANSFORM_BUCKETS,
        )

        # Phase-level latency (separate metrics with custom buckets per phase)
        self.phase_latency_fetch: Histogram = self._get_or_create_metric(
            Histogram,
            name='phase_latency_fetch_seconds',
            documentation='Time spent in fetch phase',
            labelnames=['loader'],
            namespace=ns,
            subsystem=ss,
            buckets=FETCH_BUCKETS,
        )

        self.phase_latency_transform: Histogram = self._get_or_create_metric(
            Histogram,
            name='phase_latency_transform_seconds',
            documentation='Time spent in transform phase',
            labelnames=['loader'],
            namespace=ns,
            subsystem=ss,
            buckets=TRANSFORM_BUCKETS,
        )

        self.phase_latency_write: Histogram = self._get_or_create_metric(
            Histogram,
            name='phase_latency_write_seconds',
            documentation='Time spent in write phase',
            labelnames=['loader'],
            namespace=ns,
            subsystem=ss,
            buckets=WRITE_BUCKETS,
        )

    def _get_or_create_metric(self, metric_class: type, name: str, **kwargs) -> Any:
        """Get an existing metric from the registry or create a new one.

        This handles the case where metrics might already be registered
        (e.g., during test runs) by returning the existing metric instead
        of failing with a duplicate registration error.
        """
        ns = kwargs.get('namespace', '')
        ss = kwargs.get('subsystem', '')

        # Build the full metric name as Prometheus does
        # Only strip _total suffix for Counter metrics (prometheus auto-adds it)
        metric_name = name
        if metric_class == Counter and name.endswith('_total'):
            metric_name = name[:-6]  # Strip '_total' suffix
        full_name = '_'.join(filter(None, [ns, ss, metric_name]))

        try:
            return metric_class(name=name, **kwargs)
        except ValueError as e:
            if 'Duplicated timeseries' in str(e):
                # Metric already exists, try to get it from the registry
                if full_name in REGISTRY._names_to_collectors:
                    return REGISTRY._names_to_collectors[full_name]
                # If we can't find it, re-raise the original error
                raise
            raise

    def _setup_null_metrics(self) -> None:
        """Set up no-op metrics when prometheus is unavailable or disabled."""
        self.records_processed = NullMetric()
        self.processing_latency = NullMetric()
        self.errors = NullMetric()
        self.batch_sizes = NullMetric()
        self.active_connections = NullMetric()
        self.queue_depth = NullMetric()
        self.bytes_processed = NullMetric()
        self.reorg_events = NullMetric()
        self.retry_attempts = NullMetric()
        self.build_info = NullMetric()
        # Flight SQL metrics
        self.flight_fetch_latency = NullMetric()
        self.flight_bytes_received = NullMetric()
        self.flight_batches_received = NullMetric()
        # Streaming worker metrics
        self.streaming_workers_active = NullMetric()
        self.streaming_workers_total = NullMetric()
        self.streaming_batch_wait = NullMetric()
        self.streaming_worker_processing = NullMetric()
        # Phase-level latency
        self.phase_latency_fetch = NullMetric()
        self.phase_latency_transform = NullMetric()
        self.phase_latency_write = NullMetric()

    @contextmanager
    def track_operation(
        self, loader: str, operation: str, table: str = '', connection: str = 'default'
    ) -> Iterator[Dict[str, Any]]:
        """Context manager to track an operation's duration and success.

        Usage:
            with metrics.track_operation('postgresql', 'load_batch', table='users') as ctx:
                # do work
                ctx['records'] = 1000

        Args:
            loader: Loader type name
            operation: Operation name (e.g., 'load_batch', 'connect')
            table: Target table name (optional)
            connection: Connection name (optional)

        Yields:
            Context dict where you can set 'records' and 'bytes' for automatic tracking
        """
        ctx: Dict[str, Any] = {'records': 0, 'bytes': 0, 'error': None}
        start_time = time.perf_counter()

        try:
            yield ctx
        except Exception as e:
            ctx['error'] = type(e).__name__
            self.errors.labels(loader=loader, error_type=type(e).__name__, table=table).inc()
            raise
        finally:
            duration = time.perf_counter() - start_time
            self.processing_latency.labels(loader=loader, operation=operation).observe(duration)

            if ctx['records'] > 0:
                self.records_processed.labels(loader=loader, table=table, connection=connection).inc(ctx['records'])
                self.batch_sizes.labels(loader=loader, table=table).observe(ctx['records'])

            if ctx['bytes'] > 0:
                self.bytes_processed.labels(loader=loader, table=table).inc(ctx['bytes'])

    def reset(self) -> None:
        """Reset all metrics (useful for testing).

        Note: This creates new metric instances. In production, you typically
        don't reset metrics as they should accumulate over the process lifetime.
        """
        self._initialized = False
        self._setup_metrics()

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing).

        This also unregisters metrics from Prometheus registry to allow
        re-registration with the same names.
        """
        with cls._lock:
            if cls._instance is not None and PROMETHEUS_AVAILABLE:
                from prometheus_client import REGISTRY

                # Unregister all metrics from the global registry
                metrics_to_unregister = [
                    'records_processed',
                    'processing_latency',
                    'errors',
                    'batch_sizes',
                    'active_connections',
                    'queue_depth',
                    'bytes_processed',
                    'reorg_events',
                    'retry_attempts',
                    'build_info',
                    # Flight SQL metrics
                    'flight_fetch_latency',
                    'flight_bytes_received',
                    'flight_batches_received',
                    # Streaming worker metrics
                    'streaming_workers_active',
                    'streaming_workers_total',
                    'streaming_batch_wait',
                    'streaming_worker_processing',
                    # Phase latency metrics
                    'phase_latency_fetch',
                    'phase_latency_transform',
                    'phase_latency_write',
                ]
                for metric_name in metrics_to_unregister:
                    metric = getattr(cls._instance, metric_name, None)
                    if metric is not None and not isinstance(metric, NullMetric):
                        try:
                            REGISTRY.unregister(metric)
                        except Exception:
                            pass  # Metric may not be registered
            cls._instance = None


def get_metrics(config: Optional[MetricsConfig] = None) -> AmpMetrics:
    """Get the global metrics instance.

    Args:
        config: Optional configuration (only used on first call)

    Returns:
        The singleton AmpMetrics instance
    """
    return AmpMetrics(config)


def start_metrics_server(port: int = 8000, addr: str = '') -> None:
    """Start HTTP server to expose Prometheus metrics.

    Args:
        port: Port to listen on (default: 8000)
        addr: Address to bind to (default: all interfaces)

    Raises:
        RuntimeError: If prometheus_client is not available
    """
    if not PROMETHEUS_AVAILABLE:
        raise RuntimeError('prometheus_client is not installed. Install with: pip install prometheus-client')

    start_http_server(port, addr)


def generate_metrics_text() -> bytes:
    """Generate Prometheus metrics in text format.

    Returns:
        Metrics in Prometheus text exposition format

    Raises:
        RuntimeError: If prometheus_client is not available
    """
    if not PROMETHEUS_AVAILABLE:
        raise RuntimeError('prometheus_client is not installed. Install with: pip install prometheus-client')

    return generate_latest(REGISTRY)


def is_prometheus_available() -> bool:
    """Check if prometheus_client is available.

    Returns:
        True if prometheus_client is installed and importable
    """
    return PROMETHEUS_AVAILABLE


# Convenience function for instrumenting a callable
def timed(
    metric_name: str = 'processing_latency',
    loader: str = 'unknown',
    operation: str = 'unknown',
) -> Callable:
    """Decorator to time function execution.

    Usage:
        @timed(loader='postgresql', operation='query')
        def execute_query():
            ...

    Args:
        metric_name: Name of the histogram metric to use
        loader: Loader label value
        operation: Operation label value

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            metrics = get_metrics()
            with metrics.track_operation(loader, operation):
                return func(*args, **kwargs)

        return wrapper

    return decorator
