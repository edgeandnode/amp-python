# Amp DBT - Phase 1 Implementation

## Overview

Phase 1 implements the core compilation engine for Amp DBT, providing basic query composition with Jinja templating and external dataset reference resolution.

## Features Implemented

### ✅ Project Initialization
- `amp-dbt init` command creates a new DBT project structure
- Generates `dbt_project.yml`, directory structure, and example model

### ✅ Model Loading and Parsing
- Loads SQL model files from `models/` directory
- Parses `{{ config() }}` blocks from model SQL
- Extracts configuration (dependencies, track_progress, etc.)

### ✅ Jinja Templating Support
- Full Jinja2 template rendering
- Custom `ref()` function for dependency resolution
- Support for variables and macros (basic)

### ✅ ref() Resolution (External Datasets Only)
- Resolves `{{ ref('eth') }}` to dataset references like `_/eth_firehose@1.0.0`
- Validates dependencies are defined in config
- Replaces ref() calls in compiled SQL

### ✅ Basic Config Parsing
- Parses `{{ config() }}` blocks from model SQL
- Loads `dbt_project.yml` (optional)
- Supports dependencies, track_progress, register, deploy flags

### ✅ CLI Commands
- `amp-dbt init` - Initialize new project
- `amp-dbt compile` - Compile models
- `amp-dbt list` - List all models

## Usage

### Initialize a Project

```bash
amp-dbt init my-project
cd my-project
```

### Create a Model

Create `models/staging/stg_erc20_transfers.sql`:

```sql
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
    l.address as token_address
FROM {{ ref('eth') }}.logs l
WHERE
    l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
    AND l.topic3 IS NULL
```

### Compile Models

```bash
# Compile all models
amp-dbt compile

# Compile specific models
amp-dbt compile --select stg_*

# Show compiled SQL
amp-dbt compile --show-sql
```

## Project Structure

```
my-project/
├── dbt_project.yml          # Project configuration
├── models/                  # SQL model files
│   ├── staging/
│   │   └── stg_erc20_transfers.sql
│   └── marts/
│       └── token_analytics.sql
├── macros/                  # Reusable SQL macros (future)
├── tests/                   # Data quality tests (future)
└── docs/                    # Documentation (future)
```

## Limitations (Phase 1)

- ❌ Internal model references (model-to-model dependencies) not supported
- ❌ Macros system not fully implemented
- ❌ No execution (`amp-dbt run`) - compilation only
- ❌ No monitoring or tracking
- ❌ No testing framework

## Next Steps (Phase 2)

- Internal model dependency resolution (CTE inlining)
- Dependency graph building
- Topological sort for execution order
- `amp-dbt run` command for execution

