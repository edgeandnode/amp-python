"""
Unit tests for amp metrics instrumentation.

Tests the metrics module functionality including:
- Metrics initialization and singleton pattern
- Counter, Gauge, and Histogram operations
- Context manager for tracking operations
- Graceful degradation when prometheus_client unavailable
"""

import pytest

from src.amp.metrics import (
    AmpMetrics,
    MetricsConfig,
    NullMetric,
    get_metrics,
    is_prometheus_available,
)


@pytest.mark.unit
class TestMetricsConfig:
    """Test MetricsConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = MetricsConfig()

        assert config.enabled is True
        assert config.namespace == 'amp'
        assert config.subsystem == ''
        assert config.default_labels == {}
        assert len(config.histogram_buckets) > 0

    def test_custom_values(self):
        """Test custom configuration values."""
        config = MetricsConfig(
            enabled=False,
            namespace='myapp',
            subsystem='loader',
            default_labels={'env': 'prod'},
        )

        assert config.enabled is False
        assert config.namespace == 'myapp'
        assert config.subsystem == 'loader'
        assert config.default_labels == {'env': 'prod'}


@pytest.mark.unit
class TestNullMetric:
    """Test NullMetric no-op implementation."""

    def test_null_metric_all_operations(self):
        """Test that NullMetric accepts all operations silently."""
        metric = NullMetric()

        # All operations should succeed without error
        metric.inc()
        metric.inc(10)
        metric.dec()
        metric.dec(5)
        metric.set(100)
        metric.observe(0.5)
        metric.info({'version': '1.0'})

    def test_null_metric_labels(self):
        """Test that labels() returns another NullMetric."""
        metric = NullMetric()

        labeled = metric.labels(foo='bar', baz='qux')
        assert isinstance(labeled, NullMetric)

        # Chained operations should work
        metric.labels(a='1').labels(b='2').inc()

    def test_null_metric_timer(self):
        """Test that time() returns a no-op timer."""
        metric = NullMetric()
        timer = metric.time()

        with timer:
            pass  # Should not raise

    def test_null_metric_track_inprogress(self):
        """Test that track_inprogress context manager works."""
        metric = NullMetric()

        with metric.track_inprogress():
            pass  # Should not raise


@pytest.mark.unit
class TestAmpMetricsSingleton:
    """Test AmpMetrics singleton pattern."""

    def setup_method(self):
        """Reset singleton before each test."""
        AmpMetrics.reset_instance()

    def teardown_method(self):
        """Reset singleton after each test."""
        AmpMetrics.reset_instance()

    def test_singleton_returns_same_instance(self):
        """Test that multiple calls return the same instance."""
        metrics1 = AmpMetrics()
        metrics2 = AmpMetrics()

        assert metrics1 is metrics2

    def test_get_metrics_returns_singleton(self):
        """Test that get_metrics returns the singleton."""
        metrics1 = get_metrics()
        metrics2 = get_metrics()

        assert metrics1 is metrics2

    def test_config_only_used_on_first_call(self):
        """Test that config is only applied on first initialization."""
        config1 = MetricsConfig(namespace='first')
        config2 = MetricsConfig(namespace='second')

        metrics1 = AmpMetrics(config1)
        metrics2 = AmpMetrics(config2)

        # Both should use the first config
        assert metrics1._config.namespace == 'first'
        assert metrics2._config.namespace == 'first'


@pytest.mark.unit
class TestAmpMetricsWithPrometheus:
    """Test AmpMetrics when prometheus_client is available."""

    def setup_method(self):
        """Reset singleton before each test."""
        AmpMetrics.reset_instance()

    def teardown_method(self):
        """Reset singleton after each test."""
        AmpMetrics.reset_instance()

    @pytest.mark.skipif(not is_prometheus_available(), reason='prometheus_client not installed')
    def test_metrics_initialized(self):
        """Test that metrics are properly initialized."""
        metrics = get_metrics()

        # All expected metrics should be present
        assert hasattr(metrics, 'records_processed')
        assert hasattr(metrics, 'processing_latency')
        assert hasattr(metrics, 'errors')
        assert hasattr(metrics, 'batch_sizes')
        assert hasattr(metrics, 'active_connections')
        assert hasattr(metrics, 'queue_depth')
        assert hasattr(metrics, 'bytes_processed')
        assert hasattr(metrics, 'reorg_events')
        assert hasattr(metrics, 'retry_attempts')
        assert hasattr(metrics, 'build_info')

    @pytest.mark.skipif(not is_prometheus_available(), reason='prometheus_client not installed')
    def test_counter_operations(self):
        """Test counter metric operations."""
        metrics = get_metrics()

        # Should not raise
        metrics.records_processed.labels(loader='test', table='users', connection='default').inc()
        metrics.records_processed.labels(loader='test', table='users', connection='default').inc(100)

    @pytest.mark.skipif(not is_prometheus_available(), reason='prometheus_client not installed')
    def test_gauge_operations(self):
        """Test gauge metric operations."""
        metrics = get_metrics()

        # Should not raise
        metrics.active_connections.labels(loader='test', target='localhost').inc()
        metrics.active_connections.labels(loader='test', target='localhost').dec()
        metrics.active_connections.labels(loader='test', target='localhost').set(5)

    @pytest.mark.skipif(not is_prometheus_available(), reason='prometheus_client not installed')
    def test_histogram_operations(self):
        """Test histogram metric operations."""
        metrics = get_metrics()

        # Should not raise
        metrics.processing_latency.labels(loader='test', operation='load').observe(0.5)
        metrics.batch_sizes.labels(loader='test', table='users').observe(1000)


@pytest.mark.unit
class TestAmpMetricsDisabled:
    """Test AmpMetrics when disabled."""

    def setup_method(self):
        """Reset singleton before each test."""
        AmpMetrics.reset_instance()

    def teardown_method(self):
        """Reset singleton after each test."""
        AmpMetrics.reset_instance()

    def test_disabled_metrics_use_null_metric(self):
        """Test that disabled metrics use NullMetric."""
        config = MetricsConfig(enabled=False)
        metrics = get_metrics(config)

        # All metrics should be NullMetric instances
        assert isinstance(metrics.records_processed, NullMetric)
        assert isinstance(metrics.processing_latency, NullMetric)
        assert isinstance(metrics.errors, NullMetric)

    def test_disabled_metrics_operations_succeed(self):
        """Test that operations on disabled metrics succeed silently."""
        config = MetricsConfig(enabled=False)
        metrics = get_metrics(config)

        # All operations should work without error
        metrics.records_processed.labels(loader='test', table='t', connection='c').inc(100)
        metrics.processing_latency.labels(loader='test', operation='op').observe(0.5)
        metrics.active_connections.labels(loader='test', target='t').set(10)


@pytest.mark.unit
class TestTrackOperation:
    """Test the track_operation context manager."""

    def setup_method(self):
        """Reset singleton before each test."""
        AmpMetrics.reset_instance()

    def teardown_method(self):
        """Reset singleton after each test."""
        AmpMetrics.reset_instance()

    def test_track_operation_basic(self):
        """Test basic track_operation usage."""
        config = MetricsConfig(enabled=False)  # Use NullMetric for isolated testing
        metrics = get_metrics(config)

        with metrics.track_operation('test_loader', 'test_op', table='test_table') as ctx:
            ctx['records'] = 100
            ctx['bytes'] = 5000

        # Should complete without error
        assert ctx['records'] == 100
        assert ctx['bytes'] == 5000

    def test_track_operation_error_handling(self):
        """Test track_operation records errors."""
        config = MetricsConfig(enabled=False)
        metrics = get_metrics(config)

        with pytest.raises(ValueError):
            with metrics.track_operation('test_loader', 'test_op'):
                raise ValueError('test error')

    def test_track_operation_default_values(self):
        """Test track_operation with default context values."""
        config = MetricsConfig(enabled=False)
        metrics = get_metrics(config)

        with metrics.track_operation('test_loader', 'test_op') as ctx:
            pass  # Don't set any values

        assert ctx['records'] == 0
        assert ctx['bytes'] == 0
        assert ctx['error'] is None


@pytest.mark.unit
class TestIsPrometheusAvailable:
    """Test is_prometheus_available function."""

    def test_returns_boolean(self):
        """Test that function returns a boolean."""
        result = is_prometheus_available()
        assert isinstance(result, bool)
