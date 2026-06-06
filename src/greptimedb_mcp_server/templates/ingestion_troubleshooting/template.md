# GreptimeDB Ingestion Troubleshooting

Source: `{{ source }}`
{% if table %}Table: `{{ table }}`{% endif %}
{% if symptom %}
Symptom:

```
{{ symptom }}
```
{% endif %}

## First Checks

1. Confirm the target database and table name.
2. Check whether the data landed in a different table or schema.
3. Verify timestamp precision and client/session timezone.
4. Inspect the generated schema before assuming column names.
5. For pipeline ingestion, dry-run the pipeline with representative data before writing.

## SQL Checks

```sql
SHOW TABLES;

{% if table %}
{% set _parts = table.split('.') %}
{% set schema = _parts[0] if _parts | length > 1 else '' %}
{% set tbl = _parts[1] if _parts | length > 1 else table %}
DESCRIBE {{ table }};
SHOW CREATE TABLE {{ table }};

-- After DESCRIBE identifies the time index, sample recent rows with that column.
-- Example shape: SELECT * FROM {{ table }} ORDER BY <time_index> DESC LIMIT 20;

-- Column semantic types.
SELECT column_name, data_type, semantic_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{{ tbl }}'{% if schema %} AND table_schema = '{{ schema }}'{% endif %};
{% else %}
-- Find recently created or relevant tables.
SELECT table_schema, table_name, table_id, engine
FROM INFORMATION_SCHEMA.TABLES
ORDER BY table_name;
{% endif %}
```

## Source-Specific Checks

{% if source | lower in ["otlp", "opentelemetry", "traces"] %}
- For traces, check the `x-greptime-pipeline-name: greptime_trace_v1` header and the trace table name header if customized.
- Default trace table is usually `opentelemetry_traces`.
- Trace attributes may be flattened into columns such as `span_attributes.http.route`.
{% elif source | lower in ["prometheus", "remote write", "prometheus remote write"] %}
- Confirm metric names and labels after ingestion; Prometheus labels become queryable series labels for TQL.
- Use `execute_tql` with a simple instant/range expression before debugging complex dashboards.
{% elif source | lower in ["loki", "logs", "pipeline"] %}
- For logs, inspect whether the raw message was kept or excluded by a `select` processor.
- Use `dryrun_pipeline` with `application/x-ndjson`, `application/json`, or `text/plain` matching the real payload.
- Check whether the message field has a `FULLTEXT INDEX` before relying on `@@`.
{% else %}
- Validate the write protocol payload format and table mapping.
- Check whether timestamps are seconds, milliseconds, microseconds, or nanoseconds.
{% endif %}

## Pipeline Dry Run Pattern

```python
dryrun_pipeline(
    pipeline_name="pipeline_name",
    data='{"timestamp":"2024-05-25T20:16:37Z","message":"example"}',
    data_type="application/x-ndjson"
)
```

## Output

- Classify the issue: connection/auth, table routing, schema, timestamp, pipeline parse, or query expectation.
- Give the smallest next check that can confirm or reject the hypothesis.
- Do not suggest destructive changes. If schema changes are needed, propose a new table or an explicit migration plan.

## References

- [Pipeline Configuration](https://docs.greptime.com/reference/pipeline/pipeline-config/)
- [Manage Pipelines](https://docs.greptime.com/user-guide/logs/manage-pipelines)
- [Read and Write Traces](https://docs.greptime.com/user-guide/traces/read-write)
- [SQL Query](https://docs.greptime.com/user-guide/query-data/sql)
