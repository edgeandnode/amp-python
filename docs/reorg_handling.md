# Blockchain Reorganization (Reorg) Handling in amp-python

## Overview

Blockchain reorganizations (reorgs) occur when a blockchain's canonical chain is modified, causing previously confirmed blocks to become invalid. The amp-python client library implements sophisticated reorg handling across all data loaders to ensure data consistency when loading blockchain data into various storage backends.
This document describes the reorg handling approach for each loader implementation, detailing how blockchain metadata is stored and how each backend leverages its unique features for efficient reorg processing.

## Core Concepts

### Block Range Metadata

All loaders track the blockchain origin of data using block range metadata. This metadata identifies which network and block range each piece of data came from, enabling precise deletion of affected data during a reorg.

**Standard Metadata Format:**
```json
[
  {"network": "ethereum", "start": 100, "end": 110},
  {"network": "polygon", "start": 200, "end": 210}
]
```

### Reorg Detection

When a reorg is detected on a network at a specific block number, all data with block ranges from that network where `end >= reorg_start` must be deleted to maintain consistency.

## Loader Implementations

### 1. PostgreSQL Loader

PostgreSQL leverages its powerful native JSON capabilities for optimal reorg handling.

#### Storage Strategy
- **Metadata Column**: `_meta_block_ranges` using `JSONB` data type
- **Benefits**: Native indexing, efficient queries, compression

#### Implementation
```sql
DELETE FROM table_name
WHERE EXISTS (
  SELECT 1 
  FROM jsonb_array_elements(_meta_block_ranges) AS range_elem
  WHERE range_elem->>'network' = 'ethereum' 
  AND (range_elem->>'end')::int >= 150
)
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐⭐⭐ Excellent
- **Operation**: Single SQL DELETE statement
- **Complexity**: O(n) with JSONB GIN index
- **Transaction Support**: Full ACID compliance

#### Best Practices
- Create GIN index on `_meta_block_ranges` for large tables
- Use batch operations when handling multiple reorgs
- Leverage PostgreSQL's EXPLAIN ANALYZE for query optimization

---

### 2. Redis Loader

Redis uses a sophisticated index-based approach for lightning-fast reorg handling.

#### Storage Strategy
- **Data Storage**: Hash structure with pattern `{table}:{id}`
- **Block Index**: Sorted set `{table}_block_index` for efficient range queries
- **Index Format**: Member = `{network}:{start}:{end}:{row_id}`, Score = `end_block`

#### Implementation
```python
def _handle_reorg(self, invalidation_ranges, table_name):
    index_key = f"{table_name}_block_index"
    
    for range_obj in invalidation_ranges:
        # Use sorted set range query - O(log N + M)
        entries = redis.zrangebyscore(
            index_key, 
            range_obj.start,  # min score
            '+inf'            # max score
        )
        
        # Parse and filter by network
        for entry in entries:
            network, start, end, row_id = entry.split(':')
            if network == range_obj.network:
                # Delete data and index atomically
                pipeline = redis.pipeline()
                pipeline.delete(f"{table_name}:{row_id}")
                pipeline.zrem(index_key, entry)
                pipeline.execute()
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐⭐⭐ Excellent
- **Operation**: Sorted set range query + batch delete
- **Complexity**: O(log N + M) where M is matches
- **Transaction Support**: Pipeline for atomicity

#### Best Practices
- Use Redis pipelines for atomic operations
- Consider memory limits when designing key patterns
- Monitor sorted set size for large datasets

---

### 3. Snowflake Loader

Snowflake utilizes its semi-structured data capabilities and cloud-native architecture.

#### Storage Strategy
- **Metadata Column**: `_meta_block_ranges` using `VARIANT` data type
- **Benefits**: Automatic JSON indexing, columnar compression

#### Implementation
```sql
DELETE FROM table_name
WHERE EXISTS (
  SELECT 1 
  FROM TABLE(FLATTEN(input => PARSE_JSON(_meta_block_ranges))) f
  WHERE f.value:network::string = 'ethereum'
  AND f.value:end::int >= 150
)
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐⭐ Very Good
- **Operation**: Single SQL DELETE with FLATTEN
- **Complexity**: O(n) with automatic optimization
- **Transaction Support**: Full ACID compliance

#### Best Practices
- Leverage Snowflake's automatic clustering on frequently queried columns
- Use multi-cluster warehouses for concurrent reorg operations
- Monitor credit usage for large reorg operations

---

### 4. Apache Iceberg Loader

Iceberg provides immutable snapshots and time-travel capabilities for safe reorg handling.

#### Storage Strategy
- **Metadata Column**: `_meta_block_ranges` as string column with JSON
- **Benefits**: Snapshot isolation, version history, rollback capability

#### Implementation
```python
def _handle_reorg(self, invalidation_ranges, table_name):
    # Load current snapshot
    iceberg_table = catalog.load_table(table_name)
    arrow_table = iceberg_table.scan().to_arrow()
    
    # Build keep mask
    keep_indices = []
    for i in range(arrow_table.num_rows):
        meta_json = arrow_table['_meta_block_ranges'][i].as_py()
        ranges = json.loads(meta_json)
        
        should_keep = True
        for range_obj in invalidation_ranges:
            for r in ranges:
                if (r['network'] == range_obj.network and 
                    r['end'] >= range_obj.start):
                    should_keep = False
                    break
        
        if should_keep:
            keep_indices.append(i)
    
    # Create new snapshot with filtered data
    filtered_table = arrow_table.take(keep_indices)
    iceberg_table.overwrite(filtered_table)
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐ Good
- **Operation**: Full table scan + overwrite
- **Complexity**: O(n) full scan required
- **Transaction Support**: Snapshot isolation

#### Best Practices
- Compact small files periodically to improve scan performance
- Use partition evolution for time-based data
- Leverage snapshot expiration for storage management

---

### 5. Delta Lake Loader

Delta Lake combines Parquet efficiency with ACID transactions and versioning.

#### Storage Strategy
- **Metadata Column**: `_meta_block_ranges` as string column in Parquet files
- **Benefits**: Version history, concurrent reads, schema evolution

#### Implementation
```python
def _handle_reorg(self, invalidation_ranges, table_name):
    # Load current version
    delta_table = DeltaTable(table_path)
    current_table = delta_table.to_pyarrow_table()
    
    # Build PyArrow compute mask efficiently
    keep_mask = pa.array([True] * current_table.num_rows)
    
    meta_column = current_table['_meta_block_ranges']
    for i in range(current_table.num_rows):
        meta_json = meta_column[i].as_py()
        if should_delete_row(meta_json, invalidation_ranges):
            # Update mask using PyArrow compute
            row_mask = pa.array([j == i for j in range(current_table.num_rows)])
            keep_mask = pa.compute.and_(keep_mask, pa.compute.invert(row_mask))
    
    # Write new version
    filtered_table = current_table.filter(keep_mask)
    write_deltalake(table_path, filtered_table, mode='overwrite')
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐ Good
- **Operation**: Full scan + PyArrow compute + overwrite
- **Complexity**: O(n) with PyArrow optimizations
- **Transaction Support**: ACID via Delta protocol

#### Best Practices
- Enable automatic optimization for file compaction
- Use Z-ordering on frequently filtered columns
- Monitor version history size

---

### 6. LMDB Loader

LMDB provides embedded key-value storage with memory-mapped performance.

#### Storage Strategy
- **Data Storage**: Serialized Arrow RecordBatches as values
- **Metadata**: Included within each RecordBatch
- **Key Strategy**: Configurable (by ID, pattern, or composite)

#### Implementation
```python
def _handle_reorg(self, invalidation_ranges, table_name):
    with env.begin(write=True) as txn:
        cursor = txn.cursor()
        keys_to_delete = []
        
        # First pass: identify affected keys
        for key, value in cursor:
            # Deserialize Arrow batch
            batch = pa.ipc.open_stream(value).read_next_batch()
            
            if '_meta_block_ranges' in batch.schema.names:
                meta_json = batch['_meta_block_ranges'][0].as_py()
                ranges = json.loads(meta_json)
                
                if should_delete(ranges, invalidation_ranges):
                    keys_to_delete.append(key)
        
        # Second pass: delete identified keys
        for key in keys_to_delete:
            txn.delete(key)
```

#### Performance Characteristics
- **Efficiency**: ⭐⭐⭐⭐ Very Good (local)
- **Operation**: Sequential scan + batch delete
- **Complexity**: O(n) with memory-mapped I/O
- **Transaction Support**: Single-writer ACID

#### Best Practices
- Configure appropriate map size upfront
- Use read transactions for concurrent access
- Consider key design for scan efficiency

---

## Performance Comparison Matrix

| Loader | Query Type | Reorg Speed | Storage Overhead | Concurrency | Use Case |
|--------|------------|-------------|------------------|-------------|----------|
| PostgreSQL | Indexed SQL | ⭐⭐⭐⭐⭐ | Low (JSONB) | Excellent | OLTP, Real-time |
| Redis | Sorted Set | ⭐⭐⭐⭐⭐ | Medium | Good | Cache, Hot data |
| Snowflake | SQL FLATTEN | ⭐⭐⭐⭐ | Low | Excellent | Analytics, DW |
| Iceberg | Full Scan | ⭐⭐⭐ | Low | Good | Data Lake |
| Delta Lake | Full Scan | ⭐⭐⭐ | Low | Good | Streaming, ML |
| LMDB | Key Scan | ⭐⭐⭐⭐ | Medium | Limited | Embedded, Edge |

## Implementation Guidelines

### 1. Choosing the Right Loader

- **Need fastest reorgs?** → PostgreSQL or Redis
- **Need version history?** → Iceberg or Delta Lake
- **Cloud-native analytics?** → Snowflake
- **Embedded/offline?** → LMDB

### 2. Optimizing Reorg Performance

**For SQL-based loaders:**
- Create appropriate indexes on metadata columns
- Use EXPLAIN plans to verify query efficiency
- Consider partitioning for very large tables

**For scan-based loaders:**
- Implement incremental reorg strategies
- Compact files regularly
- Consider caching recent block ranges

**For key-value loaders:**
- Design keys for efficient range scans
- Use batch operations
- Monitor memory usage

### 3. Monitoring and Alerting

Implement monitoring for:
- Reorg frequency and scope
- Processing duration
- Storage growth from versions
- Failed reorg operations

### 4. Testing Reorg Handling

Essential test scenarios:
- Empty tables/databases
- Missing metadata columns
- Concurrent reorgs
- Multi-network data
- Large-scale reorgs
- Network failures during reorg

## Future Enhancements

### Short Term
- Parallel reorg processing for scan-based loaders
- Incremental reorg strategies for large datasets
- Reorg metrics and observability

### Long Term
- Unified reorg coordination service
- Predictive reorg detection
- Automatic optimization based on reorg patterns
- Cross-loader reorg synchronization

## Conclusion

The amp-python library provides robust reorg handling across diverse storage backends, each implementation optimized for its specific strengths. By understanding these approaches, users can:

1. Choose the appropriate loader for their reorg requirements
2. Optimize performance for their specific use case
3. Implement proper monitoring and testing
4. Plan for scale and growth

The consistent metadata format and streaming interface ensure that applications can handle reorgs transparently, regardless of the underlying storage technology.