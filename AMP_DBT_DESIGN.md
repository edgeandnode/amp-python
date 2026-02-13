# Amp DBT: Design Document

## 1. Overview

### 1.1 Purpose
Amp DBT is a query composition and orchestration framework for Amp that provides:
- DBT-like SQL organization and reusability
- Dependency management for query composition
- Job monitoring and data freshness tracking
- Developer-friendly CLI and workflows

### 1.2 Key Constraints
- **No materialization**: Amp queries run on-demand; no persistent tables/views
- **Query composition**: Models compile to SQL, not materialized data
- **Dependency resolution**: `ref()` resolves to SQL composition or dataset references

### 1.3 Goals
- Familiar DBT-like syntax and patterns
- Simple mental model: models = reusable SQL queries
- Easy debugging with compiled SQL visibility
- Built-in monitoring and tracking
- Fast iteration: compile → run → test

---

## 2. Architecture

### 2.1 Core Components

```
┌─────────────────────────────────────────────────────────┐
│                    Amp DBT CLI                           │
├─────────────────────────────────────────────────────────┤
│  Compiler  │  Monitor  │  Tracker  │  Executor          │
└─────────────────────────────────────────────────────────┘
         │         │         │         │
         ▼         ▼         ▼         ▼
┌─────────────────────────────────────────────────────────┐
│              Amp Python Client                          │
│  (Query Client │ Admin Client │ Jobs Client)            │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              Amp Server                                 │
│  (Flight SQL │ Admin API │ Job Management)              │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
Model SQL (with Jinja)
    ↓
[Compiler] Resolve ref(), macros, config
    ↓
Final SQL Query
    ↓
[Executor] Execute query (or register as dataset)
    ↓
[Monitor] Track job status
    ↓
[Tracker] Update latest block/timestamp
```

---

## 3. Project Structure

### 3.1 Directory Layout

```
my-amp-project/
├── dbt_project.yml              # Project configuration
├── profiles.yml                  # Amp server connections
│
├── models/                      # SQL model files
│   ├── staging/
│   │   ├── stg_erc20_transfers.sql
│   │   └── stg_erc721_transfers.sql
│   ├── intermediate/
│   │   └── int_token_metrics.sql
│   └── marts/
│       └── token_analytics.sql
│
├── macros/                      # Reusable SQL macros
│   ├── evm_decode.sql
│   ├── block_range.sql
│   └── get_latest_block.sql
│
├── tests/                       # Data quality tests
│   ├── assert_not_null.sql
│   └── assert_unique.sql
│
├── docs/                        # Documentation
│   └── schema.yml
│
└── .amp-dbt/                    # Internal state (gitignored)
    ├── state.db                 # SQLite tracking database
    ├── jobs.json                # Active job mappings
    └── compiled/                # Compiled SQL cache
```

### 3.2 Configuration Files

**dbt_project.yml:**
```yaml
name: 'my_project'
version: '1.0.0'

# Model configurations
models:
  staging:
    +dependencies:
      eth: '_/eth_firehose@1.0.0'
    +track_progress: true
    +track_column: 'block_num'
  
  marts:
    +register: true
    +deploy: false

# Monitoring settings
monitoring:
  alert_threshold_minutes: 30
  check_interval_seconds: 60
```

**profiles.yml:**
```yaml
default:
  query_url: 'grpc://34.27.238.174:80'
  admin_url: 'http://34.27.238.174:8080'
  registry_url: 'https://api.registry.amp.staging.thegraph.com'
  auth: true  # Use auto-refreshing auth

production:
  query_url: 'grpc://prod-server:80'
  admin_url: 'http://prod-server:8080'
  auth_token: '${AMP_AUTH_TOKEN}'
```

---

## 4. Core Features

### 4.1 Query Compilation

**Model File Example:**
```sql
-- models/staging/stg_erc20_transfers.sql
{{ config(
    dependencies={'eth': '_/eth_firehose@1.0.0'},
    track_progress=true,
    track_column='block_num',
    description='Decoded ERC20 Transfer events'
) }}

SELECT
    l.block_num,
    l.block_hash,
    l.timestamp,
    l.tx_hash,
    l.address as token_address,
    evm_decode(
        l.topic1, l.topic2, l.topic3, l.data,
        'Transfer(address indexed from, address indexed to, uint256 value)'
    ) as dec
FROM {{ ref('eth') }}.logs l
WHERE
    l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
    AND l.topic3 IS NULL
```

**Compilation Process:**
1. Load model SQL
2. Parse `{{ config() }}` block
3. Resolve `{{ ref() }}` calls:
   - Internal models → Inline as CTE
   - External datasets → Use dataset reference format
4. Apply macros (`{{ macro_name() }}`)
5. Generate final SQL

**Compiled Output:**
```sql
-- Compiled: stg_erc20_transfers
SELECT
    l.block_num,
    l.block_hash,
    l.timestamp,
    l.tx_hash,
    l.address as token_address,
    evm_decode(...) as dec
FROM _/eth_firehose@1.0.0.logs l
WHERE
    l.topic0 = evm_topic('Transfer(...)')
    AND l.topic3 IS NULL
```

### 4.2 Dependency Resolution

**Strategy: Hybrid Approach**

**Internal Models** → Inline as CTE:
```sql
-- Original
SELECT * FROM {{ ref('stg_transfers') }}

-- Compiled
WITH stg_transfers AS (
    SELECT ... FROM eth.logs ...
)
SELECT * FROM stg_transfers
```

**External Datasets** → Dataset Reference:
```sql
-- Original
SELECT * FROM {{ ref('eth') }}.blocks

-- Compiled
SELECT * FROM _/eth_firehose@1.0.0.blocks
```

**Dependency Graph:**
```
stg_erc20_transfers
  └── eth (external: _/eth_firehose@1.0.0)

int_token_metrics
  ├── stg_erc20_transfers (internal: inline)
  └── prices (external: _/token_prices@1.0.0)

token_analytics
  └── int_token_metrics (internal: inline)
```

### 4.3 Macros

**Macro Example:**
```sql
-- macros/evm_decode.sql
{% macro evm_decode(topic1, topic2, topic3, data, signature) %}
    evm_decode({{ topic1 }}, {{ topic2 }}, {{ topic3 }}, {{ data }}, '{{ signature }}')
{% endmacro %}
```

**Usage:**
```sql
SELECT {{ evm_decode('l.topic1', 'l.topic2', 'l.topic3', 'l.data', 'Transfer(...)') }}
```

### 4.4 Incremental Queries

Since there's no materialization, "incremental" = smart WHERE clauses:

```sql
-- models/staging/stg_erc20_transfers.sql
{{ config(
    incremental_strategy='block_range',
    dependencies={'eth': '_/eth_firehose@1.0.0'}
) }}

SELECT * FROM {{ ref('eth') }}.logs
WHERE topic0 = evm_topic('Transfer(...)')
{% if is_incremental() %}
  AND block_num > {{ var('last_processed_block', 0) }}
{% endif %}
```

**State Management:**
- Store `last_processed_block` per model in state database
- Auto-detect from destination table if available
- Support `--full-refresh` flag to reset

---

## 5. Monitoring & Tracking

### 5.1 Job Monitoring

**Integration with Amp Jobs API:**
```python
# Uses existing JobsClient
class JobMonitor:
    def monitor_job(self, job_id: int, model_name: str):
        job = self.client.jobs.get(job_id)
        
        # Track progress
        if job.status == 'Running':
            latest_block = self._get_latest_block(model_name)
            self.tracker.update_progress(model_name, latest_block)
        
        return job
```

**Job States:**
- `Pending` → Job queued
- `Running` → Actively processing
- `Completed` → Successfully finished
- `Failed` → Error occurred
- `Stopped` → Manually stopped

### 5.2 Data Progress Tracking

**State Database Schema:**
```sql
-- .amp-dbt/state.db
CREATE TABLE model_state (
    model_name TEXT PRIMARY KEY,
    connection_name TEXT,
    latest_block INTEGER,
    latest_timestamp TIMESTAMP,
    last_updated TIMESTAMP,
    job_id INTEGER,
    status TEXT  -- 'fresh', 'stale', 'error'
);

CREATE TABLE job_history (
    job_id INTEGER PRIMARY KEY,
    model_name TEXT,
    status TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    final_block INTEGER,
    rows_processed INTEGER
);
```

**Tracking Methods:**

1. **Query Destination Table** (if loading to database):
   ```sql
   SELECT MAX(block_num) FROM stg_erc20_transfers;
   ```

2. **Job Metadata** (from Amp server):
   ```python
   job = client.jobs.get(job_id)
   # Extract latest block from job metadata
   ```

3. **Stream State** (for streaming queries):
   ```python
   # Use existing stream state tracking
   checkpoint = get_checkpoint(connection_name, table_name)
   latest_block = checkpoint.end_block
   ```

### 5.3 Freshness Monitoring

**Freshness Check:**
```python
class FreshnessMonitor:
    def check_freshness(self, model_name: str) -> FreshnessResult:
        latest = self.tracker.get_latest_timestamp(model_name)
        if not latest:
            return FreshnessResult(stale=True, reason="No data")
        
        age = datetime.now() - latest
        threshold = self.config.get_alert_threshold(model_name)
        
        return FreshnessResult(
            stale=age > threshold,
            age=age,
            latest_block=self.tracker.get_latest_block(model_name)
        )
```

**Alert Thresholds:**
- Per-model configuration
- Default: 30 minutes
- Configurable in `dbt_project.yml`

---

## 6. CLI Design

### 6.1 Core Commands

```bash
# Compilation
amp-dbt compile                    # Compile all models
amp-dbt compile --select stg_*    # Compile specific models
amp-dbt compile --show-sql         # Show compiled SQL

# Execution
amp-dbt run                        # Execute all models
amp-dbt run --select stg_*         # Run specific models
amp-dbt run --register             # Register as Amp datasets
amp-dbt run --register --deploy   # Register and deploy
amp-dbt run --incremental          # Use incremental strategy
amp-dbt run --full-refresh         # Reset incremental state

# Testing
amp-dbt test                       # Run all tests
amp-dbt test --select stg_*        # Test specific models

# Monitoring
amp-dbt monitor                    # Interactive dashboard
amp-dbt monitor --watch           # Auto-refresh
amp-dbt status                     # Data freshness check
amp-dbt status --all               # All models freshness

# Job Management
amp-dbt jobs list                  # List all jobs
amp-dbt jobs status <job_id>       # Job details
amp-dbt jobs logs <job_id>         # View job logs
amp-dbt jobs stop <job_id>         # Stop running job
amp-dbt jobs resume <model>        # Resume failed job

# Utilities
amp-dbt list                       # List all models
amp-dbt docs generate              # Generate documentation
amp-dbt docs serve                 # Serve docs locally
amp-dbt clean                      # Clean compiled cache
```

### 6.2 Command Examples

**Compile and Check:**
```bash
$ amp-dbt compile --select stg_erc20_transfers

Compiling stg_erc20_transfers...
✅ Compiled successfully

Dependencies:
  - eth: _/eth_firehose@1.0.0

Compiled SQL (first 500 chars):
SELECT l.block_num, l.block_hash, ...
```

**Run with Monitoring:**
```bash
$ amp-dbt run --select stg_erc20_transfers --monitor

Compiling stg_erc20_transfers...
✅ Compiled successfully

Registering dataset...
✅ Registered: _/stg_erc20_transfers@1.0.0

Deploying...
✅ Job started: 12345

Monitoring job 12345...
[████████░░] 45% | Block: 18,500,000 | ETA: 5m
```

**Status Check:**
```bash
$ amp-dbt status

┌─────────────────────────────────────────────────────────┐
│ Model              │ Status    │ Latest Block │ Age      │
├─────────────────────────────────────────────────────────┤
│ stg_erc20_transfers│ ✅ Fresh  │ 18,500,000  │ 5 min    │
│ stg_erc721_transfers│ ⚠️ Stale │ 18,400,000  │ 2 hours  │
│ token_analytics    │ ❌ Error  │ -           │ -        │
└─────────────────────────────────────────────────────────┘
```

**Monitor Dashboard:**
```bash
$ amp-dbt monitor

┌─────────────────────────────────────────────────────────┐
│ Amp DBT Job Monitor (Refreshing every 5s)               │
├─────────────────────────────────────────────────────────┤
│ Model              │ Job ID │ Status    │ Progress │ Block │
├─────────────────────────────────────────────────────────┤
│ stg_erc20_transfers│ 12345  │ Running   │ 45%      │ 18.5M │
│ stg_erc721_transfers│ 12346  │ Completed │ 100%     │ 18.5M │
│ token_analytics    │ 12347  │ Failed    │ 0%       │ -     │
└─────────────────────────────────────────────────────────┘

Press 'q' to quit, 'r' to refresh
```

---

## 7. Implementation Phases

### Phase 1: Core Compilation (MVP)
**Goal:** Get basic query compilation working

**Features:**
- ✅ Project initialization (`amp-dbt init`)
- ✅ Model loading and parsing
- ✅ Jinja templating support
- ✅ `ref()` resolution (external datasets only)
- ✅ Basic config parsing
- ✅ `amp-dbt compile` command

**Deliverables:**
- Compiler engine
- Project structure
- Basic CLI

### Phase 2: Dependency Resolution
**Goal:** Full dependency graph support

**Features:**
- ✅ Internal model `ref()` resolution (CTE inlining)
- ✅ Dependency graph building
- ✅ Topological sort for execution order
- ✅ Circular dependency detection
- ✅ `amp-dbt run` command

**Deliverables:**
- Dependency resolver
- Execution orchestrator
- Error handling

### Phase 3: Monitoring & Tracking
**Goal:** Job monitoring and data tracking

**Features:**
- ✅ Job status tracking
- ✅ State database (SQLite)
- ✅ Latest block/timestamp tracking
- ✅ Freshness monitoring
- ✅ `amp-dbt monitor` dashboard
- ✅ `amp-dbt status` command

**Deliverables:**
- Monitoring system
- State tracker
- Dashboard UI

### Phase 4: Advanced Features
**Goal:** Production-ready features

**Features:**
- ✅ Macros system
- ✅ Testing framework
- ✅ Documentation generation
- ✅ Incremental query support
- ✅ Dataset registration automation
- ✅ Alerts/notifications
- ✅ Performance metrics

**Deliverables:**
- Complete feature set
- Production-ready tool

---

## 8. Technical Specifications

### 8.1 Core Classes

```python
# amp_dbt/core.py
class AmpDbtProject:
    """Main project class"""
    def compile_model(self, model_name: str) -> CompiledModel
    def compile_all(self) -> Dict[str, CompiledModel]
    def build_dag(self) -> Dict[str, List[str]]
    def resolve_dependencies(self, model: str) -> Dict[str, str]

class Compiler:
    """Query compilation engine"""
    def compile(self, sql: str, context: dict) -> str
    def resolve_ref(self, ref_name: str) -> str
    def apply_macros(self, sql: str) -> str

class Executor:
    """Query execution"""
    def execute(self, model: CompiledModel) -> ExecutionResult
    def register_dataset(self, model: CompiledModel) -> int
    def deploy_dataset(self, namespace: str, name: str, version: str) -> int

class JobMonitor:
    """Job monitoring"""
    def monitor_job(self, job_id: int, model_name: str) -> JobInfo
    def monitor_all(self) -> List[JobInfo]

class ModelTracker:
    """Data progress tracking"""
    def get_latest_block(self, model_name: str) -> Optional[int]
    def update_progress(self, model_name: str, block_num: int)
    def check_freshness(self, model_name: str) -> FreshnessResult
```

### 8.2 State Management

**State Database:**
- SQLite database in `.amp-dbt/state.db`
- Tables: `model_state`, `job_history`, `compiled_cache`

**Jobs Mapping:**
- JSON file: `.amp-dbt/jobs.json`
- Maps model names to active job IDs

**Compiled Cache:**
- Directory: `.amp-dbt/compiled/`
- Stores compiled SQL for faster recompilation

### 8.3 Error Handling

**Error Types:**
- `CompilationError`: SQL compilation failed
- `DependencyError`: Missing or circular dependency
- `ExecutionError`: Query execution failed
- `JobError`: Job monitoring error
- `ConfigError`: Invalid configuration

**Error Messages:**
- Clear, actionable error messages
- Suggestions for fixes
- Link to documentation

---

## 9. Example Workflows

### 9.1 New Project Setup

```bash
# 1. Initialize project
amp-dbt init my-project
cd my-project

# 2. Configure profiles.yml
# Edit profiles.yml with Amp server URLs

# 3. Create first model
# models/staging/stg_erc20_transfers.sql
{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT * FROM {{ ref('eth') }}.logs WHERE ...

# 4. Compile and test
amp-dbt compile
amp-dbt test

# 5. Run
amp-dbt run --select stg_erc20_transfers
```

### 9.2 Daily Operations

```bash
# Morning: Check status
amp-dbt status

# Run incremental updates
amp-dbt run --incremental

# Monitor progress
amp-dbt monitor --watch

# Evening: Check for stale data
amp-dbt alerts
```

### 9.3 Debugging

```bash
# See compiled SQL
amp-dbt compile --select stg_erc20_transfers --show-sql

# Check dependencies
amp-dbt list --select stg_erc20_transfers

# View job logs
amp-dbt jobs logs <job_id>

# Test specific model
amp-dbt test --select stg_erc20_transfers
```

---

## 10. Success Metrics

### 10.1 Developer Experience
- ✅ Time to create new model: < 5 minutes
- ✅ Compilation time: < 1 second per model
- ✅ Clear error messages: 100% actionable
- ✅ Documentation coverage: 100%

### 10.2 Reliability
- ✅ Compilation success rate: > 99%
- ✅ Job monitoring accuracy: 100%
- ✅ Freshness detection accuracy: > 95%

### 10.3 Performance
- ✅ State database queries: < 10ms
- ✅ Dashboard refresh: < 1 second
- ✅ CLI command response: < 2 seconds

---

## 11. Future Enhancements

### 11.1 Advanced Features
- Query performance optimization hints
- Automatic query rewriting for optimization
- Multi-environment support (dev/staging/prod)
- CI/CD integration
- Slack/email notifications

### 11.2 Integration
- dbt Cloud integration
- Airflow/Dagster integration
- Data quality frameworks (Great Expectations)
- BI tool connectors

---

## 12. Appendix

### 12.1 File Format Examples

**Model File:**
```sql
-- models/staging/stg_erc20_transfers.sql
{{ config(
    dependencies={'eth': '_/eth_firehose@1.0.0'},
    track_progress=true,
    track_column='block_num',
    description='Decoded ERC20 Transfer events'
) }}

SELECT ...
FROM {{ ref('eth') }}.logs
WHERE ...
```

**Macro File:**
```sql
-- macros/evm_decode.sql
{% macro evm_decode(topic1, topic2, topic3, data, signature) %}
    evm_decode({{ topic1 }}, {{ topic2 }}, {{ topic3 }}, {{ data }}, '{{ signature }}')
{% endmacro %}
```

**Test File:**
```sql
-- tests/assert_not_null.sql
SELECT COUNT(*) as null_count
FROM {{ ref('stg_erc20_transfers') }}
WHERE token_address IS NULL
-- Fails if null_count > 0
```

### 12.2 Configuration Reference

**dbt_project.yml:**
```yaml
name: 'my_project'
version: '1.0.0'

models:
  staging:
    +dependencies:
      eth: '_/eth_firehose@1.0.0'
    +track_progress: true
    +track_column: 'block_num'
  
  marts:
    +register: true
    +deploy: false

monitoring:
  alert_threshold_minutes: 30
  check_interval_seconds: 60
```

---

## Summary

Amp DBT provides:
1. **Query Composition**: DBT-like SQL organization with dependency management
2. **Monitoring**: Real-time job tracking and data freshness monitoring
3. **Developer Experience**: Simple CLI, clear errors, fast iteration
4. **Production Ready**: State tracking, error handling, comprehensive tooling

The design balances familiarity (DBT patterns) with practicality (Amp constraints) to create a tool that makes working with Amp queries easy and reliable.

