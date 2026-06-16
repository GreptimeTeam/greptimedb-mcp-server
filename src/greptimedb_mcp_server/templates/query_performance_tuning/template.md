# GreptimeDB Query Performance Tuning

{% if table %}Table: `{{ table }}`{% endif %}
{% if symptom %}
Symptom:

```
{{ symptom }}
```
{% endif %}

Query:

```sql
{{ query }}
```

## Tuning Flow

1. Run `explain_query(query=..., analyze=false)` first.
2. If the query is safe and bounded by time or `LIMIT`, run
   `explain_query(query=..., analyze=true)`.
   For scan-level metrics (per-partition index pruning, row-group
   filtering), run `explain_query(query=..., analyze=true, verbose=true)`.
3. Use `describe_table` or `SHOW CREATE TABLE` only to confirm the time index, primary key, and indexed columns needed to interpret the plan.
4. Check whether time filters, primary key ordering, partition pruning, and
   secondary indexes can be used.
5. Choose the right query form:
   - `execute_tql` for PromQL-compatible metric expressions.
   - `query_range` or SQL `RANGE` for regular time-window aggregation.
   - Plain SQL for joins, CTEs, log search, and trace drill-down.
6. If the plan is fine but latency is still high, check storage-engine metrics
   such as read stage latency and cache hit/miss before changing SQL.

## Diagnostics

```sql
{% if table %}
DESCRIBE {{ table }};
SHOW CREATE TABLE {{ table }};
{% else %}
-- Identify the table first, then run DESCRIBE and SHOW CREATE TABLE.
SHOW TABLES;
{% endif %}
```

## Common Fixes

- Add or tighten a time filter on the time index.
- Avoid selecting wide rows when only a few columns are needed.
- Use filters that match the primary key order and partition key when possible.
- For text term or phrase search, prefer `@@`/`matches_term()` on a fulltext-indexed column when that matches the query semantics.
- For latest point per series, consider `SELECT DISTINCT ON (...) ... ORDER BY ..., ts DESC`.
- For regular rollups, prefer `RANGE ... ALIGN ... BY (...)` over hand-written bucket logic when it matches the use case.
- If object storage reads dominate and cache misses are high, review Mito cache
  sizing instead of rewriting a query that already prunes well.
- If the plan points to missing locality or missing indexes, switch to `schema_design_advisor` instead of changing schema from this prompt.

## Plan Evidence

Look for these signals in `EXPLAIN ANALYZE VERBOSE` output:

- `MergeScanExec` peers and region count.
- `SeqScan` projection and filters.
- `partition_count`, memtable ranges, and file ranges.
- `rows_before_filter` versus rows after fulltext, inverted, bloom, and precise filters.
- `rg_fulltext_filtered`, `rg_inverted_filtered`, `rg_minmax_filtered`, and `rg_bloom_filtered`.
- `scan_cost`, `build_parts_cost`, `build_reader_cost`, and `output_rows`.

Useful runtime metrics:

- `greptime_mito_read_stage_elapsed_bucket`
- `greptime_mito_cache_bytes`
- `greptime_mito_cache_hit`
- `greptime_mito_cache_miss`

## Output

- Explain the most likely bottleneck.
- Show the plan evidence to look for.
- Propose one query rewrite.
- Mention runtime metrics only when the query plan suggests storage or cache pressure.
- State explicitly when the issue should be escalated to schema design.

## References

- [EXPLAIN Query](https://docs.greptime.com/reference/sql/explain)
- [RANGE Query](https://docs.greptime.com/reference/sql/range)
- [TQL Reference](https://docs.greptime.com/reference/sql/tql)
- [SQL Query](https://docs.greptime.com/user-guide/query-data/sql)
- [Performance Tuning Tips](https://docs.greptime.com/user-guide/deployments-administration/performance-tuning/performance-tuning-tips)
