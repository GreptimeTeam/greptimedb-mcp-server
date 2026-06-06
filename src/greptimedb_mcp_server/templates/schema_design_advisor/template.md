# GreptimeDB Schema Design Advisor

Workload: `{{ workload }}`

{% if sample_data %}
## Sample Data

```
{{ sample_data }}
```
{% endif %}

{% if query_patterns %}
## Query Patterns

```
{{ query_patterns }}
```
{% endif %}

## Goal

Design or review a GreptimeDB table schema for this workload. Prefer a simple
schema that matches real query patterns over a generic wide-table design.

## Design Checklist

1. Identify the time index column first.
2. Decide whether the workload needs deduplication or deletion:
   - If not, prefer `append_mode = 'true'` as the baseline.
   - If yes, keep append mode disabled and deduplicate by `(primary key, time index)`.
3. Separate low-cardinality dimensions from high-cardinality identifiers.
4. Use `PRIMARY KEY` only when ordering by those columns benefits common queries
   or when deduplication by those columns is required.
5. Keep the primary key short. More than five primary key columns usually hurts
   ingestion performance and memory usage.
6. Choose the SST format deliberately:
   - Prefer the default `flat` format for most workloads, including high-cardinality primary keys.
   - Consider `primary_key` format only for low-cardinality primary keys and measured benefit.
   - If deduplication on high-cardinality primary keys is not required, prefer append-only.
7. For metrics collected at the same time, prefer a wider table with multiple
   fields instead of many single-field tables when the data naturally belongs together.
8. Use secondary indexes deliberately:
   - `INVERTED INDEX` for frequently filtered low-cardinality columns.
   - `SKIPPING INDEX WITH(type='BLOOM')` for precise lookup on sparse or high-cardinality fields such as `trace_id`, `request_id`, or `device_id`.
   - `FULLTEXT INDEX` for large text fields queried with `@@` or `matches_term()`.
   - Do not add extra indexes to the time index column.
9. Start without extra indexes when query performance is already acceptable.
   Indexes add storage cost and may slow flush, compaction, and ingestion.
10. Do not add table partitioning unless one node or one partition is not enough.
11. Do not partition by time. GreptimeDB already partitions data by the time
    index at the storage layer.
12. If partitioning is needed, choose stable, evenly distributed keys that also
    appear in common query filters.

## Suggested Output

- Proposed `CREATE TABLE` statement.
- Rationale for `TIME INDEX`, `append_mode`, `merge_mode` if relevant,
  `PRIMARY KEY`, SST format, partitioning, and each index.
- Columns that should remain unindexed.
- Risks and assumptions, especially cardinality and deduplication assumptions.
- Validation queries: `DESCRIBE`, `SHOW CREATE TABLE`, and index metadata checks after data is written.

## Useful Queries

```sql
-- Inspect an existing table.
DESCRIBE table_name;
SHOW CREATE TABLE table_name;

-- Check column semantic types.
SELECT column_name, data_type, semantic_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'table_name';

-- Check SST and index metadata after data is written.
SELECT i.index_type, i.target_json, COUNT(*) as files, SUM(i.index_file_size) as index_bytes
FROM INFORMATION_SCHEMA.SSTS_INDEX_META i
JOIN INFORMATION_SCHEMA.TABLES t ON i.table_id = t.table_id
WHERE t.table_name = 'table_name'
GROUP BY i.index_type, i.target_json;
```

## References

- [Data Model](https://docs.greptime.com/user-guide/concepts/data-model)
- [CREATE TABLE](https://docs.greptime.com/reference/sql/create)
- [Data Index](https://docs.greptime.com/user-guide/manage-data/data-index)
- [Table Design Best Practices](https://docs.greptime.com/user-guide/deployments-administration/performance-tuning/design-table/)
