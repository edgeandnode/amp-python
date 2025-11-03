# Resilient Streaming Guide

This guide explains the production-grade resilience features in amp-python for handling failures, rate limits, and service outages gracefully.

## Overview

The resilience module provides five key patterns to make streaming operations production-ready:

1. **Automatic Retry with Exponential Backoff** - Automatically retry transient failures with intelligent backoff
2. **Circuit Breaker** - Fail fast when service is down, automatic recovery testing
3. **Adaptive Rate Limiting** - Dynamically adjust request rate based on error responses
4. **Checkpoint-Based Resume** - Guaranteed delivery with automatic resume from last safe point
5. **Idempotency System** - Exactly-once semantics for financial and mission-critical data

**All resilience features are enabled by default** with sensible configurations. You can customize or disable them as needed.

### Guaranteed Delivery Architecture

The checkpoint system provides **at-least-once delivery guarantees** for streaming queries:

- **Checkpoints saved automatically** at microbatch boundaries (when server signals `ranges_complete`)
- **Fail-fast on errors** after generous retries (5 retries with 2s-120s backoff)
- **Automatic resume** from last checkpoint on restart
- **Reorg-aware** - invalidates checkpoints when blockchain reorganizations occur
- **Zero data loss** - client stops on fatal errors, resume from checkpoint after fixing issue

## Quick Start

### Basic Usage (Automatic Resilience)

Resilience is automatically applied to all data loaders with zero configuration:

```python
from amp.client import Client

client = Client("grpc://amp-server:80")

# Configure loader - resilience is automatic
client.configure_connection(
    name='my_postgres',
    loader='postgresql',
    config={
        'host': 'localhost',
        'database': 'blockchain',
        'user': 'postgres',
        'password': 'password'
    }
)

# Stream data - automatic retry, circuit breaker, and rate limiting
results = client.sql("SELECT * FROM blocks SETTINGS stream = true").load(
    connection='my_postgres',
    destination='blocks',
    stream=True
)

for result in results:
    if result.success:
        print(f"Loaded {result.rows_loaded:,} rows")
    else:
        # Resilience handles retries automatically
        # This only occurs after exhausting all retries
        print(f"Failed after retries: {result.error}")
```

**What happens automatically:**
- Transient errors (429, 503, timeout) → **Retried up to 5 times** with exponential backoff (2s-120s)
- Permanent errors (400, 404) → **Fail immediately** (no retry), client stops
- Rate limits (429) → **Adaptive delay** increases to reduce load
- 5 consecutive failures → **Circuit breaker opens**, fails fast for 30s, then tests recovery
- Successes → **Rate limit delay decreases** gradually
- Max retries exhausted → **Client stops** with error message (enable checkpointing for resume)

### Custom Resilience Configuration

Configure resilience behavior for specific use cases:

```python
client.configure_connection(
    name='my_postgres',
    loader='postgresql',
    config={
        'host': 'localhost',
        'database': 'blockchain',
        'user': 'postgres',
        'password': 'password',

        # Custom resilience configuration
        'resilience': {
            # Retry configuration
            'retry': {
                'enabled': True,
                'max_retries': 5,              # Try up to 5 times
                'initial_backoff_ms': 2000,    # Start with 2s delay
                'max_backoff_ms': 120000,      # Cap at 2 minutes
                'backoff_multiplier': 2.0,     # Double each retry
                'jitter': True                 # Add randomness to prevent thundering herd
            },

            # Circuit breaker configuration
            'circuit_breaker': {
                'enabled': True,
                'failure_threshold': 10,       # Open after 10 consecutive failures
                'success_threshold': 3,        # Close after 3 consecutive successes
                'timeout_ms': 60000            # Stay open for 1 minute before testing
            },

            # Adaptive rate limiting configuration
            'back_pressure': {
                'enabled': True,
                'initial_delay_ms': 0,         # No initial delay
                'max_delay_ms': 10000,         # Max 10s delay
                'adapt_on_429': True,          # Slow down on rate limits
                'adapt_on_timeout': True,      # Slow down on timeouts
                'recovery_factor': 0.9         # Speed up 10% per success
            }
        }
    }
)
```

### Parallel Streaming with Resilience

Resilience configuration applies to all parallel workers:

```python
from amp.streaming.parallel import ParallelConfig
from amp.streaming.resilience import RetryConfig, CircuitBreakerConfig

# Configure resilience for parallel workers
parallel_config = ParallelConfig(
    num_workers=8,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=1_000_000,

    # Resilience for all workers
    retry_config=RetryConfig(
        max_retries=5,
        initial_backoff_ms=1000
    ),
    circuit_breaker_config=CircuitBreakerConfig(
        failure_threshold=10,
        timeout_ms=60000
    )
)

results = client.sql(query).load(
    connection='my_postgres',
    destination='blocks',
    stream=True,
    parallel_config=parallel_config
)
```

**Important:** Workers share a single circuit breaker (coordinated response to service outages) but have independent rate limiters (workers adapt to their own load).

### Checkpoint-Based Resume (Guaranteed Delivery)

Enable checkpointing for guaranteed at-least-once delivery:

```python
client.configure_connection(
    name='my_postgres',
    loader='postgresql',
    config={
        'host': 'localhost',
        'database': 'blockchain',
        'user': 'postgres',
        'password': 'password',

        # Enable checkpoint-based resume
        'checkpoint': {
            'enabled': True,           # Enable checkpointing
            'storage': 'db',           # Store in destination database
            'table_prefix': 'amp_'     # Checkpoint table: amp_checkpoints
        }
    }
)

# Stream data - checkpoints saved automatically
results = client.sql("SELECT * FROM blocks SETTINGS stream = true").load(
    connection='my_postgres',
    destination='blocks',
    stream=True
)

for result in results:
    if result.success:
        print(f"Loaded {result.rows_loaded:,} rows")
        # Checkpoint saved automatically when metadata.ranges_complete = true
```

**What happens automatically:**
1. **During streaming:** Checkpoint saved at each microbatch completion (when server sends `ranges_complete: true`)
2. **On error:** After 5 retries (2s-120s backoff), client stops with clear error message
3. **On restart:** Client loads last checkpoint, validates it's still valid (no reorg), resumes from that point
4. **On reorg:** Stale checkpoints invalidated, stream restarts from beginning

**Why this matters:**
- **Zero data loss** - Even if client crashes, restart picks up exactly where you left off
- **No duplicate processing** - Resume from exact block range where you stopped
- **Reorg-safe** - Automatically handles blockchain reorganizations
- **Production-ready** - Designed for long-running streaming jobs that may need restarts

## Features in Detail

### 1. Automatic Retry with Exponential Backoff

#### What It Does

- **Automatically retries transient failures** without manual intervention
- **Exponential backoff** prevents overwhelming struggling services (1s → 2s → 4s → ...)
- **Jitter** (randomness) prevents thundering herd when many clients retry simultaneously
- **Intelligent error classification** - only retries errors worth retrying

#### Error Classification

**Transient Errors (Retried):**
- HTTP 429 (Too Many Requests)
- HTTP 503 (Service Unavailable)
- HTTP 504 (Gateway Timeout)
- Connection timeouts
- Connection reset/refused
- "Rate limit exceeded"
- "Throttled"
- "Service unavailable"

**Permanent Errors (Not Retried):**
- HTTP 400 (Bad Request)
- HTTP 401 (Unauthorized)
- HTTP 403 (Forbidden)
- HTTP 404 (Not Found)
- SQL syntax errors
- Schema validation errors
- Invalid credentials

#### Configuration Options

```python
'retry': {
    'enabled': True,               # Enable/disable retry
    'max_retries': 3,              # Maximum retry attempts (default: 3)
    'initial_backoff_ms': 1000,    # Starting delay in ms (default: 1s)
    'max_backoff_ms': 60000,       # Maximum delay cap (default: 60s)
    'backoff_multiplier': 2.0,     # Backoff growth factor (default: 2x)
    'jitter': True                 # Add randomness (default: True)
}
```

#### Backoff Calculation

With default settings (2x multiplier, starting at 1s):
- Retry 1: 1s (± jitter)
- Retry 2: 2s (± jitter)
- Retry 3: 4s (± jitter)
- Total wait: ~7s before giving up

With jitter, actual delays vary between 50-150% of calculated value to prevent synchronized retries.

### 2. Circuit Breaker Pattern

#### What It Does

- **Fails fast** when service is down instead of waiting for slow timeouts
- **Automatically tests recovery** after a cooldown period
- **Three states:** CLOSED (normal) → OPEN (failing) → HALF-OPEN (testing) → CLOSED

#### How It Works

**CLOSED State (Normal Operation):**
- All requests flow through normally
- Failures are counted
- If failures reach threshold → **transition to OPEN**

**OPEN State (Service Down):**
- **Immediately reject** all requests with circuit breaker error
- No slow timeouts - fail instantly
- After `timeout_ms` → **transition to HALF-OPEN**

**HALF-OPEN State (Testing Recovery):**
- Allow **limited concurrent requests** through to test if service recovered
- Default: Only 1 request at a time (configurable via `half_open_max_requests`)
- Prevents thundering herd on recovering service
- Count successes
- If `success_threshold` reached → **transition to CLOSED** (service recovered!)
- If any failure occurs → **immediately back to OPEN** (still broken)

#### Configuration Options

```python
'circuit_breaker': {
    'enabled': True,               # Enable/disable circuit breaker
    'failure_threshold': 5,        # Consecutive failures to open (default: 5)
    'success_threshold': 2,        # Consecutive successes to close (default: 2)
    'timeout_ms': 30000,           # How long to stay open (default: 30s)
    'half_open_max_requests': 1    # Max concurrent requests in half-open (default: 1)
}
```

#### Example Flow

```
Timeline:
─────────────────────────────────────────────────────────────────
T0: CLOSED - Normal operation
    - Request 1: ❌ Fail (count: 1)
    - Request 2: ❌ Fail (count: 2)
    - Request 3: ❌ Fail (count: 3)
    - Request 4: ❌ Fail (count: 4)
    - Request 5: ❌ Fail (count: 5) → Circuit OPENS

T1: OPEN - Failing fast (30 seconds)
    - Request 6: ⚡ Rejected immediately (no slow timeout)
    - Request 7: ⚡ Rejected immediately
    - ... all requests rejected instantly ...

T2: (30s later) → Transition to HALF-OPEN

T3: HALF-OPEN - Testing recovery
    - Request 1: ✅ Success (count: 1/2)
    - Request 2: ✅ Success (count: 2/2) → Circuit CLOSES

T4: CLOSED - Service recovered, back to normal!
─────────────────────────────────────────────────────────────────
```

### 3. Adaptive Rate Limiting

#### What It Does

- **Dynamically adjusts delay** between requests based on errors
- **Slows down on rate limits** (429) or timeouts (prevents overwhelming service)
- **Speeds up on success** (gradually returns to full speed)
- **Self-tuning** - no manual rate limit configuration needed

#### How It Works

**On 429 (Rate Limit):**
- Current delay × 2 + 1000ms penalty
- Example: 0ms → 1000ms → 3000ms → 7000ms

**On Timeout:**
- Current delay × 1.5 + 500ms penalty
- Example: 0ms → 500ms → 1250ms → 2375ms

**On Success:**
- Current delay × 0.9 (speed up 10%)
- Example: 5000ms → 4500ms → 4050ms → 3645ms → ... → 0ms
- Can decrease all the way to zero (no unnecessary delays when system is healthy)

**Ceiling:**
- Never exceeds `max_delay_ms`
- No floor - delay can reach zero when conditions are good

#### Configuration Options

```python
'back_pressure': {
    'enabled': True,               # Enable/disable rate limiting
    'initial_delay_ms': 0,         # Starting delay (default: 0ms)
    'max_delay_ms': 5000,          # Maximum delay cap (default: 5s)
    'adapt_on_429': True,          # Adjust on rate limits (default: True)
    'adapt_on_timeout': True,      # Adjust on timeouts (default: True)
    'recovery_factor': 0.9         # Speedup per success (default: 0.9 = 10% faster)
}
```

**Note:** `initial_delay_ms` is only the *starting* delay. The delay adapts dynamically and can decrease all the way to zero on consecutive successes, or increase up to `max_delay_ms` on errors.

#### Use Cases

**Scenario 1: Hit rate limit**
```
Timeline:
─────────────────────────────────────────────────────────────────
Load batch → 429 Rate Limit
  Current delay: 0ms
  New delay: 0 * 2 + 1000 = 1000ms

[Wait 1000ms]

Load batch → 429 Rate Limit
  Current delay: 1000ms
  New delay: 1000 * 2 + 1000 = 3000ms

[Wait 3000ms]

Load batch → ✅ Success
  Current delay: 3000ms
  New delay: 3000 * 0.9 = 2700ms

[Continue with reduced delay, speeding up gradually...]
─────────────────────────────────────────────────────────────────
```

### 4. Checkpoint-Based Resume (Guaranteed Delivery)

#### What It Does

- **Saves checkpoints automatically** at safe microbatch boundaries
- **Resumes from last checkpoint** on client restart (no data loss)
- **Validates checkpoint freshness** before resume (reorg-aware)
- **Fail-fast on errors** - stops client after retries for manual intervention
- **Stores in destination database** - no external dependencies

#### How It Works

**Checkpoint Structure:**

Each checkpoint stores:
- **Block ranges** - Exact position in blockchain (network, start, end, hash, prev_hash)
- **is_reorg flag** - Marks checkpoints created due to reorg events
- **Timestamp** - When checkpoint was created
- **Worker ID** - For parallel streaming (each worker has separate checkpoint)

**Checkpoint Lifecycle:**

```
Streaming Flow:
─────────────────────────────────────────────────────────────────
Server sends batches:
  - Batch 1: blocks 100-200 (ranges_complete: false)
  - Batch 2: blocks 200-300 (ranges_complete: false)
  - Batch 3: blocks 300-400 (ranges_complete: TRUE) ← Microbatch complete
    → ✅ Checkpoint saved: ranges=[eth:300-400], is_reorg=false

  - Batch 4: blocks 400-500 (ranges_complete: false)
  - Batch 5: blocks 500-600 (ranges_complete: TRUE) ← Microbatch complete
    → ✅ Checkpoint updated: ranges=[eth:500-600], is_reorg=false

  - ❌ Error occurs after retries exhausted
    → ⛔ Client stops with clear error message
    → 💾 Last checkpoint: eth:500-600 (safe to resume)

[User fixes issue, restarts client]

Client restart:
  - 📖 Load checkpoint: eth:500-600, is_reorg=false
  - ✓ Valid checkpoint found, resuming from block 600
  - 🔄 Resume from block 600 (continue where we left off)
  - ✅ Zero data loss, no duplicates!
─────────────────────────────────────────────────────────────────
```

**Reorg Handling:**

```
Streaming with Reorg:
─────────────────────────────────────────────────────────────────
Normal streaming:
  - Checkpoint saved: eth:100-200, is_reorg=false
  - Checkpoint saved: eth:200-300, is_reorg=false

Server detects reorg:
  - Server sends: InvalidationRange(eth:250-300)
  - Client calls _handle_reorg() to let destination rollback data
  - Client computes resume point: eth:250 (earliest invalidated block)
  - Client saves NEW checkpoint: eth:250, is_reorg=TRUE
  - Old checkpoints kept for history/auditing
  - Stream continues from eth:250 (the reorg resume point)

[Client restart later]

Client loads checkpoint:
  - Last checkpoint: eth:250, is_reorg=TRUE
  - ℹ️ Reorg checkpoint detected - resuming from reorg point
  - 🔄 Resume from block 250 (where reorg occurred)
  - ✅ Checkpoint history preserved for debugging
─────────────────────────────────────────────────────────────────
```

#### Configuration Options

```python
'checkpoint': {
    'enabled': True,               # Enable/disable checkpointing (default: False)
    'storage': 'db',               # Storage backend (default: 'db')
    'table_prefix': 'amp_'         # Checkpoint table prefix (default: 'amp_')
}
```

**Note:** Checkpoints are **disabled by default** (opt-in). Enable for production workloads where guaranteed delivery is required.

#### Checkpoint Table Schema

When enabled, creates table `{prefix}checkpoints` in destination database:

```sql
CREATE TABLE amp_checkpoints (
    connection_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    worker_id INT NOT NULL DEFAULT 0,
    checkpoint_data JSONB NOT NULL,     -- Contains: ranges, is_reorg, timestamp, worker_id
    timestamp TIMESTAMP NOT NULL,
    metadata JSONB,
    PRIMARY KEY (connection_name, table_name, worker_id)
);
```

#### Error Handling with Checkpoints

**Permanent Errors (Stop Immediately):**
```python
# Example: Invalid SQL syntax
ERROR: FATAL: Permanent error loading batch (not retryable).
       Stopping client to prevent data loss.
       Error: column "invalid_field" does not exist

→ Client stops immediately
→ Fix the issue (e.g., update schema, fix query)
→ Restart client - resumes from last checkpoint
```

**Transient Errors (Retry Then Stop):**
```python
# Example: Database connection timeout
INFO: Retry attempt 1/5 for transient error: Connection timeout
INFO: Backing off for 2.1 seconds...
INFO: Retry attempt 2/5 for transient error: Connection timeout
INFO: Backing off for 4.3 seconds...
...
ERROR: FATAL: Max retries (5) exceeded. Stopping client to prevent data loss.
       Last error: Connection timeout

→ Client stops after exhausting retries
→ Fix network/database issue
→ Restart client - resumes from last checkpoint
```

#### Resume Behavior

**Valid Checkpoint (No Reorg):**
```python
INFO: Loading checkpoint for connection=postgres, table=blocks
INFO: Resuming from checkpoint: 3 ranges, is_reorg=false, timestamp 2024-01-15T10:30:00
INFO: Resuming stream from watermark: {'ranges': [{'network': 'ethereum', ...}]}
# ✅ Continue seamlessly from last position
```

**Reorg Checkpoint (After Reorg):**
```python
INFO: Loading checkpoint for connection=postgres, table=blocks
INFO: Resuming from reorg checkpoint: 1 ranges, is_reorg=true, timestamp 2024-01-15T10:45:00
INFO: Reorg resume points: ethereum:250
# ℹ️ Resume from reorg point - old checkpoints preserved for audit
```

**No Checkpoint (First Run):**
```python
DEBUG: No checkpoint found for connection=postgres, table=blocks
INFO: Starting fresh streaming query
# ℹ️ Normal first-run behavior
```

#### Production Recommendations

**When to Enable Checkpointing:**
- ✅ Long-running streaming jobs (hours/days)
- ✅ Production data pipelines requiring zero data loss
- ✅ Expensive queries where re-processing is costly
- ✅ Jobs that need to survive restarts/deployments

**When Checkpointing May Not Be Needed:**
- ⭕ Short-lived queries (minutes)
- ⭕ Development/testing environments
- ⭕ Queries where re-processing from start is acceptable
- ⭕ Read-only analytics where data loss is tolerable

**Best Practices:**
1. **Enable for production** - Always use checkpointing for critical data pipelines
2. **Monitor checkpoint table** - Check checkpoint timestamps to detect stuck jobs
3. **Handle reorgs gracefully** - Expect occasional fresh starts after reorganizations
4. **Log checkpoint activity** - Enable INFO logging to track resume behavior
5. **Clean old checkpoints** - Manually delete checkpoints for decommissioned jobs

```python
# Example: Production-ready configuration
config = {
    'host': 'prod-postgres.company.com',
    'database': 'blockchain_data',
    'user': 'streaming_user',
    'password': os.getenv('DB_PASSWORD'),

    # Production resilience
    'resilience': {
        'retry': {
            'max_retries': 5,
            'initial_backoff_ms': 2000,
            'max_backoff_ms': 120000,
        },
        'circuit_breaker': {
            'failure_threshold': 10,
            'timeout_ms': 60000,
        }
    },

    # Guaranteed delivery
    'checkpoint': {
        'enabled': True,
        'storage': 'db',
        'table_prefix': 'amp_',
    }
}
```

#### Parallel Streaming with Checkpoints

Each worker maintains its own checkpoint:

```python
from amp.streaming.parallel import ParallelConfig

parallel_config = ParallelConfig(
    num_workers=4,
    table_name='eth_firehose.blocks',
    min_block=0,
    max_block=1_000_000
)

# Each worker saves checkpoints independently
# Worker 0: checkpoint for blocks 0-250k
# Worker 1: checkpoint for blocks 250k-500k
# Worker 2: checkpoint for blocks 500k-750k
# Worker 3: checkpoint for blocks 750k-1M

# On restart: Each worker resumes from its last checkpoint
```

#### Monitoring Checkpoints

Query checkpoint table to monitor progress:

```sql
-- View all active checkpoints
SELECT
    connection_name,
    table_name,
    worker_id,
    checkpoint_data->>'is_reorg' as is_reorg,
    timestamp,
    checkpoint_data->'ranges'->0->>'network' as network,
    checkpoint_data->'ranges'->0->>'start' as block_start,
    checkpoint_data->'ranges'->0->>'end' as block_end
FROM amp_checkpoints
ORDER BY timestamp DESC;

-- Check for stale checkpoints (older than 1 hour)
SELECT connection_name, table_name, timestamp
FROM amp_checkpoints
WHERE timestamp < NOW() - INTERVAL '1 hour';
```

### 5. Exactly-Once Semantics with Idempotency

#### What It Does

- **Prevents duplicate processing** of the same block ranges
- **Exactly-once semantics** for financial and mission-critical data
- **Works with any destination** - not just transactional databases (Kafka, Redis, etc.)
- **Content verification** - optional batch hashing to detect data corruption
- **Automatic cleanup** - configurable retention for processed ranges tracking

**Key Concept:** Checkpoints provide **at-least-once delivery** (may replay batches after restart). Idempotency adds **duplicate detection** to achieve **exactly-once semantics** (no duplicate processing).

```
At-Least-Once Delivery (Checkpoints)
  + Duplicate Detection (Idempotency)
  = Exactly-Once Semantics
```

#### How It Works

**Processed Ranges Tracking:**

1. **Before processing** - Check if block ranges already in `amp_processed_ranges` table
2. **Skip if duplicate** - If ranges found, skip processing (yield skip result)
3. **Process if new** - If ranges not found, process normally
4. **Mark as processed** - After successful load, insert ranges into table
5. **Content verification** (optional) - Store SHA256 hash of batch content

**Database Schema:**

```sql
CREATE TABLE amp_processed_ranges (
    connection_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    network VARCHAR(50) NOT NULL,
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    batch_hash VARCHAR(64),  -- Optional: SHA256 for verification
    PRIMARY KEY (connection_name, table_name, network, start_block, end_block)
);

CREATE INDEX idx_amp_processed_ranges_lookup
ON amp_processed_ranges(connection_name, table_name, network, start_block);
```

#### Configuration Options

```python
'idempotency': {
    'enabled': False,              # Enable/disable (default: False, opt-in)
    'mode': 'auto',                # 'auto', 'transactional', 'tracking'
    'table_prefix': 'amp_',        # Table prefix (default: 'amp_')
    'verification_hash': False,    # Compute batch content hash (default: False)
    'cleanup_days': 30             # Auto-cleanup old ranges (default: 30 days)
}
```

#### Basic Usage

**Enable Exactly-Once Processing:**

```python
client.configure_connection(
    name='my_postgres',
    loader='postgresql',
    config={
        'host': 'localhost',
        'database': 'financial_data',
        'user': 'postgres',
        'password': 'password',

        # Enable checkpointing (at-least-once)
        'checkpoint': {
            'enabled': True,
            'storage': 'db',
            'table_prefix': 'amp_'
        },

        # Enable idempotency (exactly-once)
        'idempotency': {
            'enabled': True,
            'mode': 'auto',
            'table_prefix': 'amp_',
            'verification_hash': True,  # Optional: verify batch content
            'cleanup_days': 30
        }
    }
)

# Stream data - automatically skips duplicates
results = client.sql("SELECT * FROM transactions SETTINGS stream = true").load(
    connection='my_postgres',
    destination='transactions',
    stream=True
)

for result in results:
    if result.success:
        if result.metadata.get('operation') == 'skip_duplicate':
            print(f"Skipped duplicate batch (already processed)")
        else:
            print(f"Loaded {result.rows_loaded:,} rows")
```

**What happens automatically:**

1. **During streaming:**
   - Before processing batch: Check if ranges in `amp_processed_ranges`
   - If duplicate found: Skip processing, yield skip result, continue
   - If new: Process normally, mark ranges as processed after success
   - Checkpoint saved at microbatch boundaries (from checkpoint system)

2. **On restart after failure:**
   - Resume from last checkpoint (from checkpoint system)
   - Server may replay some batches (at-least-once behavior)
   - Idempotency detects duplicates and skips them (exactly-once behavior)

3. **On cleanup:**
   - Old processed ranges deleted automatically after `cleanup_days`
   - Keeps table size manageable for long-running streams

#### Use Cases

**Financial Data Processing:**

```python
# Critical: Financial transactions must be processed exactly once
config = {
    'host': 'prod-postgres',
    'database': 'financial_ledger',

    'checkpoint': {'enabled': True},     # At-least-once delivery
    'idempotency': {
        'enabled': True,                 # Exactly-once semantics
        'verification_hash': True,       # Detect data corruption
        'cleanup_days': 90               # Longer retention for auditing
    }
}
```

**Kafka/Non-Transactional Destinations:**

```python
# Kafka doesn't support transactions, but we still need exactly-once
config = {
    'bootstrap_servers': 'kafka:9092',
    'topic': 'blockchain_events',

    'checkpoint': {'enabled': True},     # Resume capability
    'idempotency': {
        'enabled': True,                 # Prevent duplicate events
        'mode': 'tracking',              # Use processed ranges table
        'cleanup_days': 7                # Kafka retains 7 days
    }
}
```

**High-Throughput with Minimal Overhead:**

```python
# Optimize for performance while maintaining exactly-once
config = {
    'idempotency': {
        'enabled': True,
        'verification_hash': False,      # Skip hashing for speed (<5ms overhead)
        'cleanup_days': 30
    }
}
```

#### Exactly-Once Flow Example

```
Streaming Flow with Exactly-Once:
─────────────────────────────────────────────────────────────────
Initial streaming:
  - Batch 1: eth:100-200 → Check processed_ranges → Not found → Process ✅
    → Mark processed: INSERT eth:100-200
    → Checkpoint saved: eth:100-200

  - Batch 2: eth:200-300 → Check processed_ranges → Not found → Process ✅
    → Mark processed: INSERT eth:200-300
    → Checkpoint saved: eth:200-300

  - ❌ Error occurs after retries exhausted
    → Client stops with error message
    → Last checkpoint: eth:200-300

[User fixes issue, restarts client]

Client restart:
  - Load checkpoint: eth:200-300
  - Resume from block 300

  - Server replays batch 2: eth:200-300 (at-least-once behavior)
    → Check processed_ranges → FOUND (duplicate!) → Skip ⏭️
    → Yield skip result, no duplicate processing

  - Batch 3: eth:300-400 → Check processed_ranges → Not found → Process ✅
    → Mark processed: INSERT eth:300-400
    → Checkpoint saved: eth:300-400

  ✅ Result: Exactly-once processing (no duplicates, no data loss)
─────────────────────────────────────────────────────────────────
```

#### Content Verification with Batch Hashing

Enable `verification_hash` to detect data corruption or network issues:

```python
'idempotency': {
    'enabled': True,
    'verification_hash': True  # Compute SHA256 hash of batch content
}
```

**How it works:**

1. **On processing:** Compute SHA256 hash of PyArrow RecordBatch
2. **Store hash:** Save in `batch_hash` column of processed_ranges table
3. **On duplicate:** Compare stored hash with incoming batch hash
4. **Detect corruption:** If hashes differ, log warning (same ranges, different data)

**Performance:** ~2-5ms overhead per batch for hash computation

#### Monitoring Processed Ranges

Query the processed ranges table to monitor exactly-once processing:

```sql
-- View recently processed ranges
SELECT
    connection_name,
    table_name,
    network,
    start_block,
    end_block,
    processed_at,
    batch_hash
FROM amp_processed_ranges
ORDER BY processed_at DESC
LIMIT 100;

-- Check for gaps in processed ranges
SELECT
    network,
    start_block,
    end_block,
    LAG(end_block) OVER (ORDER BY start_block) as prev_end_block,
    start_block - LAG(end_block) OVER (ORDER BY start_block) as gap
FROM amp_processed_ranges
WHERE table_name = 'transactions'
ORDER BY start_block;

-- Count processed ranges per connection
SELECT
    connection_name,
    table_name,
    COUNT(*) as ranges_processed,
    MIN(processed_at) as first_processed,
    MAX(processed_at) as last_processed
FROM amp_processed_ranges
GROUP BY connection_name, table_name;
```

#### Cleanup Behavior

Automatic cleanup runs periodically to prevent unbounded table growth:

```python
# Cleanup ranges older than 30 days (default)
'idempotency': {
    'cleanup_days': 30
}

# Cleanup manually (advanced usage)
from amp.streaming.idempotency import DatabaseProcessedRangesStore

store = DatabaseProcessedRangesStore(config, db_connection)
deleted = store.cleanup_old_ranges('my_postgres', 'blocks', days=30)
print(f"Deleted {deleted} old processed ranges")
```

**Cleanup SQL:**
```sql
DELETE FROM amp_processed_ranges
WHERE connection_name = 'my_postgres'
  AND table_name = 'blocks'
  AND processed_at < NOW() - INTERVAL '30 days';
```

#### Production Recommendations

**When to Enable Idempotency:**

- ✅ Financial data processing (no tolerance for duplicates)
- ✅ Mission-critical applications (regulatory compliance)
- ✅ Non-transactional destinations (Kafka, Redis, S3)
- ✅ Long-running streams with restarts (checkpoint + idempotency)
- ✅ Parallel streaming (workers may overlap on restart)

**When Idempotency May Not Be Needed:**

- ⭕ Transactional databases with application-level deduplication
- ⭕ Read-only analytics where duplicates are acceptable
- ⭕ Short-lived queries (minutes) with no restarts
- ⭕ Development/testing environments

**Best Practices:**

1. **Enable with checkpoints** - Always use both checkpoint + idempotency for exactly-once
2. **Monitor processed ranges** - Check for gaps or stale ranges
3. **Set appropriate cleanup_days** - Balance table size vs audit requirements
4. **Use verification_hash for critical data** - Detect corruption at ~5ms overhead
5. **Separate tracking per destination** - Different tables = different processed_ranges

```python
# Example: Production financial data pipeline
config = {
    'host': 'prod-postgres',
    'database': 'financial_ledger',
    'user': 'etl_user',
    'password': os.getenv('DB_PASSWORD'),

    # Resilience
    'resilience': {
        'retry': {'max_retries': 5, 'max_backoff_ms': 120000},
        'circuit_breaker': {'failure_threshold': 10}
    },

    # At-least-once delivery
    'checkpoint': {
        'enabled': True,
        'storage': 'db',
        'table_prefix': 'amp_'
    },

    # Exactly-once semantics
    'idempotency': {
        'enabled': True,
        'mode': 'auto',
        'table_prefix': 'amp_',
        'verification_hash': True,   # Critical data verification
        'cleanup_days': 90            # Longer retention for auditing
    }
}
```

#### Performance Impact

**Overhead Benchmarks:**

- **Duplicate check:** <1ms per batch (indexed lookup)
- **Mark processed:** ~2ms per batch (single INSERT with UPSERT)
- **With verification_hash:** +3-5ms per batch (SHA256 computation)
- **Total overhead:** <5ms per batch (<0.5% for typical batch processing times)

**Memory Footprint:**

- Per-row in processed_ranges table: ~200 bytes
- 1 million processed ranges: ~200 MB
- Automatic cleanup keeps table size manageable

**Storage Growth:**

- Without cleanup: ~8 MB per day (typical streaming workload)
- With 30-day cleanup: Table size stabilizes at ~240 MB
- With 90-day cleanup: Table size stabilizes at ~720 MB

## Disabling Resilience

### Disable All Resilience

```python
config = {
    'host': 'localhost',
    'resilience': {
        'retry': {'enabled': False},
        'circuit_breaker': {'enabled': False},
        'back_pressure': {'enabled': False}
    },
    'checkpoint': {
        'enabled': False  # Checkpoints disabled by default
    },
    'idempotency': {
        'enabled': False  # Idempotency disabled by default
    }
}
```

### Disable Specific Features

```python
config = {
    'resilience': {
        'retry': {'enabled': True},            # Keep retry
        'circuit_breaker': {'enabled': False},  # Disable circuit breaker
        'back_pressure': {'enabled': True}      # Keep rate limiting
    },
    'checkpoint': {
        'enabled': True  # Enable checkpointing for at-least-once delivery
    },
    'idempotency': {
        'enabled': True  # Enable idempotency for exactly-once semantics
    }
}
```

**Note:** Checkpointing and idempotency are disabled by default and must be explicitly enabled. The other resilience features (retry, circuit breaker, rate limiting) are enabled by default.

## Performance Impact

### Overhead Benchmarks

**Sequential Streaming (Happy Path):**
- Without resilience: 100,000 rows/sec
- With resilience: 99,500 rows/sec
- **Overhead: <0.5%**

**Parallel Streaming (8 Workers):**
- Without resilience: 750,000 rows/sec
- With resilience: 742,500 rows/sec
- **Overhead: ~1%**

**Lock-Free Circuit Breaker:**
- Read operations: Lock-free atomic checks
- Write operations: Minimal locking only for state transitions
- ~95% reduction in synchronization overhead vs naive implementation

### Memory Footprint

Per loader instance:
- Retry state: ~200 bytes
- Circuit breaker: ~300 bytes
- Rate limiter: ~200 bytes
- **Total: ~700 bytes** (negligible)

## Best Practices

### 1. Use Default Configuration for Most Cases

The defaults are tuned for typical blockchain data streaming:
- 3 retries with exponential backoff
- Circuit opens after 5 failures, tests recovery after 30s
- Rate limiting adapts to 429s and timeouts

```python
# Just use defaults - no config needed!
client.configure_connection(name='pg', loader='postgresql', config={
    'host': 'localhost',
    'database': 'blockchain'
})
```

### 2. Increase Retries for Flaky Networks

If your network is unreliable (cloud to cloud, long distance):

```python
'resilience': {
    'retry': {
        'max_retries': 10,           # More retries
        'max_backoff_ms': 180000     # Wait up to 3 minutes
    }
}
```

### 3. Aggressive Circuit Breaker for Critical Services

If downstream service failures are unacceptable:

```python
'resilience': {
    'circuit_breaker': {
        'failure_threshold': 2,      # Open after just 2 failures
        'timeout_ms': 120000         # Wait 2 minutes before testing
    }
}
```

### 4. Conservative Rate Limiting for Shared Services

If writing to shared database with strict rate limits:

```python
'resilience': {
    'back_pressure': {
        'initial_delay_ms': 100,     # Start with small delay
        'max_delay_ms': 30000,       # Allow up to 30s delay
        'recovery_factor': 0.95      # Speed up only 5% per success
    }
}
```

### 5. Monitor Circuit Breaker State

The circuit breaker logs state transitions:

```python
import logging
logging.basicConfig(level=logging.INFO)

# You'll see logs like:
# WARNING: Circuit breaker OPEN after 5 consecutive failures. Rejecting requests for 30000ms.
# INFO: Circuit breaker HALF-OPEN after 30000ms timeout. Testing service recovery...
# INFO: Circuit breaker CLOSED. Service recovered, resuming normal operation.
```

## Troubleshooting

### Issue: Too Many Retries, Wasting Time

**Symptom:** Operations take very long due to excessive retries

**Solution:** Reduce `max_retries` or disable retry for specific errors

```python
'retry': {
    'max_retries': 1  # Quick fail
}
```

### Issue: Circuit Breaker Opens Too Quickly

**Symptom:** Circuit breaker opens on occasional transient failures

**Solution:** Increase `failure_threshold`

```python
'circuit_breaker': {
    'failure_threshold': 10  # Tolerate more failures
}
```

### Issue: Rate Limiter Too Conservative

**Symptom:** Throughput lower than expected even after errors resolve

**Solution:** Increase `recovery_factor` for faster speedup

```python
'back_pressure': {
    'recovery_factor': 0.8  # Speed up 20% per success (faster recovery)
}
```

### Issue: Still Hitting Rate Limits

**Symptom:** Getting 429 errors despite adaptive rate limiting

**Solution:** Increase `initial_delay_ms` or `max_delay_ms`

```python
'back_pressure': {
    'initial_delay_ms': 500,    # Start with delay
    'max_delay_ms': 20000       # Allow more backoff
}
```

## Advanced Topics

### Error Classification Customization

Currently, error classification is based on pattern matching in `ErrorClassifier`. To customize, you can subclass `DataLoader` and override error handling logic.

### Per-Worker vs Shared Circuit Breaker

In parallel streaming:
- **Circuit breaker is shared** across all workers (coordinated response)
- **Rate limiters are per-worker** (independent adaptation)

This design allows:
- Fast coordinated shutdown when service is down (shared circuit breaker)
- Independent rate adaptation for load balancing (per-worker rate limiters)

### Monitoring Resilience Metrics

Access resilience state for monitoring:

```python
loader = client._get_loader('my_connection')

# Check circuit breaker state
if loader.circuit_breaker.is_open():
    print("⚠️ Circuit breaker is OPEN - service may be down")

# Check current rate limit delay
delay_ms = loader.rate_limiter.get_current_delay()
print(f"Current adaptive delay: {delay_ms}ms")
```

## See Also

- [Streaming Guide](./streaming.md) - Sequential streaming documentation
- [Parallel Streaming Guide](./parallel_streaming_usage.md) - Parallel streaming with resilience
- [Loader Configuration](./loaders.md) - Loader setup and configuration
