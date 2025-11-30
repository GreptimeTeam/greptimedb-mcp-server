# Table Diagnostics: {{ table }}

Analyze table structure, region health, storage, and query performance.

## Available Tools

- `describe_table` - Get table schema
- `explain_query` - Analyze query execution plan (set `analyze=true` for runtime stats)
- `execute_sql` - Run diagnostic SQL queries

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
SELECT region_id, peer_id, is_leader, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ table }}';

-- Region statistics (rows, disk usage)
SELECT r.region_id, r.disk_size, r.memtable_size, r.num_rows
FROM INFORMATION_SCHEMA.REGION_STATISTICS r
JOIN INFORMATION_SCHEMA.TABLES t ON r.table_id = t.table_id
WHERE t.table_name = '{{ table }}';

-- Find unhealthy regions
SELECT region_id, peer_id, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ table }}' AND status != 'READY';
```

## Storage Analysis

```sql
-- SST file details
SELECT file_id, file_size, num_rows, min_ts, max_ts
FROM INFORMATION_SCHEMA.GREPTIME_REGION_PEERS p
JOIN INFORMATION_SCHEMA.SSTS_MANIFEST s ON p.region_id = s.region_id
WHERE p.table_name = '{{ table }}';

-- Index information
SELECT index_file_path, index_type, index_file_size
FROM INFORMATION_SCHEMA.SSTS_INDEX_META
WHERE region_id IN (
    SELECT region_id FROM INFORMATION_SCHEMA.REGION_PEERS
    WHERE table_name = '{{ table }}'
);
```

## Query Optimization

Use `explain_query` tool for query analysis:

```
# Basic execution plan
explain_query(query="SELECT * FROM {{ table }} WHERE ts > now() - INTERVAL '1 hour'")

# With runtime stats (actual execution)
explain_query(query="SELECT * FROM {{ table }} LIMIT 100", analyze=true)
```

**What to look for:**
- Full table scans vs index usage
- Partition pruning effectiveness
- Join strategies and row estimates

## Cluster Overview

```sql
-- Node topology
SELECT peer_id, peer_type, peer_addr, node_status
FROM INFORMATION_SCHEMA.CLUSTER_INFO;

-- Running queries
SELECT id, query, start_timestamp, elapsed_time
FROM INFORMATION_SCHEMA.PROCESSLIST;
```
