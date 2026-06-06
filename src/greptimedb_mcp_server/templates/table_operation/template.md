# Table Diagnostics: {{ table }}

{% set _parts = table.split('.') %}
{% set schema = _parts[0] if _parts | length > 1 else '' %}
{% set tbl = _parts[1] if _parts | length > 1 else table %}
Analyze table structure, region health, storage metadata, and cluster state.
For slow query analysis, use the `query_performance_tuning` prompt instead.

When `{{ table }}` is schema-qualified, INFORMATION_SCHEMA filters below match on
`table_name` plus `table_schema`; `DESCRIBE` and `SHOW CREATE TABLE` accept the
qualified name directly.

## Available Tools

- `describe_table` - Inspect table profile: schema, semantic metadata, sample rows
- `explain_query` - Analyze query execution plan (set `analyze=true` for runtime stats)
- `execute_sql` - Run diagnostic SQL queries

## Schema Analysis

```sql
-- Table profile
-- Use describe_table(table="{{ table }}") for schema, semantics, and sample rows.

-- Full DDL
SHOW CREATE TABLE {{ table }};

-- Table semantic metadata, if supported by this GreptimeDB version
SELECT table_schema, table_name, signal_type, source, pipeline, metadata_quality, semantic_options
FROM information_schema.table_semantics
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %};

-- Column details
SELECT column_name, data_type, semantic_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %};

-- Table metadata, including table_id for joins with storage tables
SELECT table_catalog, table_schema, table_name, table_id, engine
FROM INFORMATION_SCHEMA.TABLES
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %};
```

## Region Health

```sql
-- Region distribution and status
SELECT region_id, peer_id, is_leader, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %};

-- Region statistics (rows, disk usage)
SELECT r.region_id, r.disk_size, r.memtable_size, r.region_rows
FROM INFORMATION_SCHEMA.REGION_STATISTICS r
JOIN INFORMATION_SCHEMA.TABLES t ON r.table_id = t.table_id
WHERE t.table_name = '{{ tbl }}'{% if schema %} AND t.table_schema = '{{ schema }}'{% endif %};

-- Find unhealthy regions. Current statuses are usually ALIVE or DOWNGRADED.
SELECT region_id, peer_id, status
FROM INFORMATION_SCHEMA.REGION_PEERS
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %} AND status != 'ALIVE';
```

## Storage Analysis

```sql
-- SST file details for the table
SELECT s.file_id, s.file_size, s.num_rows, s.min_ts, s.max_ts, s.level, s.visible
FROM INFORMATION_SCHEMA.SSTS_MANIFEST s
JOIN INFORMATION_SCHEMA.TABLES t ON s.table_id = t.table_id
WHERE t.table_name = '{{ tbl }}'{% if schema %} AND t.table_schema = '{{ schema }}'{% endif %};

-- Index information for the table
SELECT i.index_file_path, i.index_type, i.index_file_size, i.target_json, i.meta_json
FROM INFORMATION_SCHEMA.SSTS_INDEX_META i
JOIN INFORMATION_SCHEMA.TABLES t ON i.table_id = t.table_id
WHERE t.table_name = '{{ tbl }}'{% if schema %} AND t.table_schema = '{{ schema }}'{% endif %};
```

## Cluster Overview

```sql
-- Node topology
SELECT peer_id, peer_type, peer_addr, version, uptime, node_status
FROM INFORMATION_SCHEMA.CLUSTER_INFO;

-- Running queries
SELECT id, query, start_timestamp, elapsed_time
FROM INFORMATION_SCHEMA.PROCESS_LIST;
```

## References

- [INFORMATION_SCHEMA](https://docs.greptime.com/reference/sql/information-schema/overview) - System tables overview
- [CLUSTER_INFO](https://docs.greptime.com/reference/sql/information-schema/cluster-info) - Node topology and status
- [REGION_PEERS](https://docs.greptime.com/reference/sql/information-schema/region-peers) - Region distribution and health
- [REGION_STATISTICS](https://docs.greptime.com/reference/sql/information-schema/region-statistics) - Region disk and memory usage
- [EXPLAIN Query](https://docs.greptime.com/reference/sql/explain) - Query execution plan analysis
- [SHOW Statements](https://docs.greptime.com/reference/sql/show) - SHOW CREATE TABLE and other statements
