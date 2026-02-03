# Metrics Instrumentation

This document describes the metrics instrumentation available in amp for observability and monitoring.

## Overview

Amp provides Prometheus-compatible metrics for monitoring data loading operations. The metrics module offers:

- **Low overhead** instrumentation with optional prometheus_client dependency
- **Graceful degradation** when prometheus_client is not installed
- **Consistent naming** following Prometheus conventions
- **Thread-safe** singleton implementation

## Installation

The metrics module works without prometheus_client (using no-op metrics), but to enable actual metric collection:

```bash
# Install with metrics support
pip install amp[metrics]

# Or install prometheus_client directly
pip install prometheus-client
```

## Quick Start

```python
from amp.metrics import get_metrics, start_metrics_server

# Get the global metrics instance
metrics = get_metrics()

# Start HTTP server on port 8000 for Prometheus scraping
start_metrics_server(port=8000)

# Record metrics in your code
metrics.records_processed.labels(
    loader='postgresql',
    table='users',
    connection='default'
).inc(1000)

metrics.processing_latency.labels(
    loader='postgresql',
    operation='load_batch'
).observe(0.5)
```

## Available Metrics

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `amp_records_processed_total` | loader, table, connection | Total records processed |
| `amp_errors_total` | loader, error_type, table | Total errors by type |
| `amp_bytes_processed_total` | loader, table | Total bytes processed |
| `amp_reorg_events_total` | loader, network, table | Blockchain reorg events |
| `amp_retry_attempts_total` | loader, operation, reason | Retry attempts |

### Histograms

| Metric | Labels | Description |
|--------|--------|-------------|
| `amp_processing_latency_seconds` | loader, operation | Processing time distribution |
| `amp_batch_size_records` | loader, table | Batch size distribution |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `amp_active_connections` | loader, target | Current active connections |
| `amp_queue_depth` | queue_name | Current queue depth |

### Info

| Metric | Labels | Description |
|--------|--------|-------------|
| `amp_build_info` | (various) | Build/version information |

## Context Manager for Operations

The `track_operation` context manager simplifies instrumentation:

```python
from amp.metrics import get_metrics

metrics = get_metrics()

with metrics.track_operation('postgresql', 'load_batch', table='users') as ctx:
    # Your loading code here
    rows_loaded = load_data(batch)

    # Set context for automatic metric recording
    ctx['records'] = rows_loaded
    ctx['bytes'] = batch.nbytes

# Metrics are automatically recorded:
# - processing_latency is observed
# - records_processed is incremented
# - bytes_processed is incremented
# - errors are recorded if an exception occurs
```

## Configuration

Customize metrics collection with `MetricsConfig`:

```python
from amp.metrics import get_metrics, MetricsConfig

config = MetricsConfig(
    enabled=True,                    # Enable/disable all metrics
    namespace='amp',                 # Metric name prefix
    subsystem='loader',              # Optional subsystem name
    default_labels={'env': 'prod'},  # Default labels for all metrics
    histogram_buckets=(              # Custom latency buckets
        0.001, 0.005, 0.01, 0.025, 0.05,
        0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ),
)

metrics = get_metrics(config)
```

## Prometheus Integration

### HTTP Endpoint

Start a metrics server for Prometheus scraping:

```python
from amp.metrics import start_metrics_server

# Start on default port 8000
start_metrics_server()

# Or specify custom port and address
start_metrics_server(port=9090, addr='0.0.0.0')
```

### Generate Metrics Text

Generate metrics in Prometheus text format for custom export:

```python
from amp.metrics import generate_metrics_text

# Get metrics as bytes
metrics_text = generate_metrics_text()

# Use in your HTTP handler
@app.route('/metrics')
def metrics_endpoint():
    return Response(generate_metrics_text(), mimetype='text/plain')
```

### Example Prometheus Config

```yaml
scrape_configs:
  - job_name: 'amp'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 15s
```

## Grafana Dashboard

Example queries for a Grafana dashboard:

```promql
# Records processed rate (per second)
rate(amp_records_processed_total[5m])

# P99 latency
histogram_quantile(0.99, rate(amp_processing_latency_seconds_bucket[5m]))

# Error rate percentage
rate(amp_errors_total[5m]) / rate(amp_records_processed_total[5m]) * 100

# Active connections by loader
amp_active_connections

# Average batch size
rate(amp_batch_size_records_sum[5m]) / rate(amp_batch_size_records_count[5m])
```

## Graceful Degradation

When prometheus_client is not installed, the metrics module uses no-op implementations that silently accept all operations:

```python
from amp.metrics import get_metrics, is_prometheus_available

if is_prometheus_available():
    print("Prometheus metrics enabled")
else:
    print("Metrics disabled - install prometheus-client to enable")

# Code works the same either way
metrics = get_metrics()
metrics.records_processed.labels(loader='test', table='t', connection='c').inc(100)
```

## Testing

For testing, you can reset the metrics singleton:

```python
from amp.metrics import AmpMetrics

def test_my_loader():
    # Reset before test
    AmpMetrics.reset_instance()

    # Run test with fresh metrics
    metrics = get_metrics()
    # ...

    # Clean up after test
    AmpMetrics.reset_instance()
```

## Best Practices

1. **Use consistent labels** - Keep label values consistent across your codebase
2. **Avoid high cardinality** - Don't use user IDs or request IDs as labels
3. **Use track_operation** - Prefer the context manager for automatic error handling
4. **Set up alerts** - Configure Prometheus alerts for error rates and latency
5. **Dashboard first** - Design your metrics around what you want to see in dashboards
