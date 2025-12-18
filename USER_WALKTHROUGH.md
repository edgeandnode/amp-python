# Complete User Walkthrough: Amp DBT from Start to Finish

## Step-by-Step Guide

### Step 1: Initialize Project

```bash
# Create directory
mkdir -p /tmp/my-amp-project

# Initialize (from project root)
cd /Users/vivianpeng/Work/amp-python
uv run python -m amp.dbt.cli init --project-dir /tmp/my-amp-project
```

**Creates:** models/, macros/, tests/, docs/, dbt_project.yml

---

### Step 2: Create Models

#### Model 1: Staging (External Dependency)

```bash
cd /tmp/my-amp-project
mkdir -p models/staging

cat > models/staging/stg_erc20.sql << 'EOF'
{{ config(dependencies={'eth': '_/eth_firehose@1.0.0'}) }}
SELECT block_num, tx_hash FROM {{ ref('eth') }}.logs LIMIT 100
EOF
```

#### Model 2: Intermediate (Internal Dependency)

```bash
mkdir -p models/intermediate

cat > models/intermediate/int_stats.sql << 'EOF'
SELECT COUNT(*) as count FROM {{ ref('stg_erc20') }}
EOF
```

#### Model 3: Marts (Internal Dependency)

```bash
mkdir -p models/marts

cat > models/marts/analytics.sql << 'EOF'
SELECT * FROM {{ ref('int_stats') }} ORDER BY count DESC LIMIT 10
EOF
```

---

### Step 3: Test/Compile

```bash
cd /Users/vivianpeng/Work/amp-python

# List models
uv run python -m amp.dbt.cli list --project-dir /tmp/my-amp-project

# Compile
uv run python -m amp.dbt.cli compile --project-dir /tmp/my-amp-project

# See compiled SQL
uv run python -m amp.dbt.cli compile --show-sql --project-dir /tmp/my-amp-project
```

**Shows:** Dependencies, execution order, compiled SQL with CTEs

---

### Step 4: Run

```bash
# Dry run (see plan)
uv run python -m amp.dbt.cli run --dry-run --project-dir /tmp/my-amp-project

# Actually run
uv run python -m amp.dbt.cli run --project-dir /tmp/my-amp-project
```

**Creates:** State tracking in .amp-dbt/state.db

---

### Step 5: Monitor

```bash
# Check status
uv run python -m amp.dbt.cli status --project-dir /tmp/my-amp-project

# Monitor dashboard
uv run python -m amp.dbt.cli monitor --project-dir /tmp/my-amp-project

# Auto-refresh
uv run python -m amp.dbt.cli monitor --watch --project-dir /tmp/my-amp-project
```

---

## Quick Command Reference

All commands run from: `/Users/vivianpeng/Work/amp-python`

```bash
# Initialize
uv run python -m amp.dbt.cli init --project-dir /tmp/my-amp-project

# List
uv run python -m amp.dbt.cli list --project-dir /tmp/my-amp-project

# Compile
uv run python -m amp.dbt.cli compile --project-dir /tmp/my-amp-project
uv run python -m amp.dbt.cli compile --show-sql --project-dir /tmp/my-amp-project

# Run
uv run python -m amp.dbt.cli run --dry-run --project-dir /tmp/my-amp-project
uv run python -m amp.dbt.cli run --project-dir /tmp/my-amp-project

# Monitor
uv run python -m amp.dbt.cli status --project-dir /tmp/my-amp-project
uv run python -m amp.dbt.cli monitor --project-dir /tmp/my-amp-project
```
