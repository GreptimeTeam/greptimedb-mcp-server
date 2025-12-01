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
SELECT r.region_id, r.disk_size, r.memtable_size, r.region_rows
FROM INFORMATION_SCHEMA.REGION_STATISTICS r
JOIN INFORMATION_SCHEMA.TABLES t ON r.table_id = t.table_id
WHERE t.table_name = '{{ table }}';

-- Find unhealthy regions (status should be ALIVE)
SELECT region_id, peer_id, status, down_seconds
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ table }}' AND status != 'ALIVE';
```

## Storage Analysis

```sql
-- SST file details for the table
SELECT s.file_id, s.file_size, s.num_rows, s.min_ts, s.max_ts, s.level
FROM INFORMATION_SCHEMA.SSTS_MANIFEST s
JOIN INFORMATION_SCHEMA.TABLES t ON s.table_id = t.table_id
WHERE t.table_name = '{{ table }}';

-- Index information for the table
SELECT i.index_file_path, i.index_type, i.index_file_size, i.target_json
FROM INFORMATION_SCHEMA.SSTS_INDEX_META i
JOIN INFORMATION_SCHEMA.TABLES t ON i.table_id = t.table_id
WHERE t.table_name = '{{ table }}';
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
SELECT peer_id, peer_type, peer_addr, version, uptime, node_status
FROM INFORMATION_SCHEMA.CLUSTER_INFO;

-- Running queries
SELECT id, query, start_timestamp, elapsed_time
FROM INFORMATION_SCHEMA.PROCESSLIST;
```

## References

- [INFORMATION_SCHEMA](https://docs.greptime.com/reference/sql/information-schema/overview) - System tables overview
- [CLUSTER_INFO](https://docs.greptime.com/reference/sql/information-schema/cluster-info) - Node topology and status
- [REGION_PEERS](https://docs.greptime.com/reference/sql/information-schema/region-peers) - Region distribution and health
- [REGION_STATISTICS](https://docs.greptime.com/reference/sql/information-schema/region-statistics) - Region disk and memory usage
- [EXPLAIN Query](https://docs.greptime.com/reference/sql/explain) - Query execution plan analysis
- [SHOW Statements](https://docs.greptime.com/reference/sql/show) - SHOW CREATE TABLE and other statements
