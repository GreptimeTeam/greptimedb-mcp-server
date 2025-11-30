# Table Diagnostics: {{ table }}

Analyze table structure, region health, storage, and query performance.

## Schema Analysis

```sql
-- Table structure
DESCRIBE {{ table }};

-- Full DDL
SHOW CREATE TABLE {{ table }};

-- Column details
SELECT column_name, data_type, semantic_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{{ table }}';
```

## Region Health

```sql
-- Region distribution and status
SELECT region_id, peer_id, role, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ table }}';

-- Region statistics (rows, disk usage)
SELECT region_id, disk_size, memtable_size, num_rows
FROM INFORMATION_SCHEMA.REGION_STATISTICS
WHERE table_name = '{{ table }}';

-- Find unhealthy regions
SELECT region_id, peer_id, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ table }}' AND status != 'READY';
```

## Storage Analysis

```sql
-- SST file details
SELECT file_id, file_size, num_rows, time_range
FROM INFORMATION_SCHEMA.SSTS_MANIFEST
WHERE table_name = '{{ table }}';

-- Index information
SELECT index_id, index_type, index_size
FROM INFORMATION_SCHEMA.SSTS_INDEX_META
WHERE table_name = '{{ table }}';
```

## Query Optimization

```sql
-- Execution plan (replace with actual query)
EXPLAIN SELECT * FROM {{ table }} WHERE ts > now() - INTERVAL '1 hour';

-- Detailed analysis with runtime stats
EXPLAIN ANALYZE SELECT * FROM {{ table }} LIMIT 100;
```

## Cluster Overview

```sql
-- Node topology
SELECT peer_id, peer_type, status, hostname
FROM INFORMATION_SCHEMA.CLUSTER_INFO;

-- Running queries
SELECT id, query, start_time, elapsed_time
FROM INFORMATION_SCHEMA.PROCESS_LIST;
```
