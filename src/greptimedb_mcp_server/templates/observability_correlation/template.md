# Observability Correlation

{% set traces = trace_table | default("opentelemetry_traces", true) %}
Time range: `{{ start_time }}` to `{{ end_time }}`
{% if service_name %}Service: `{{ service_name }}`{% endif %}
{% if trace_id %}Trace ID: `{{ trace_id }}`{% endif %}
Trace table used in examples: `{{ traces }}`
{% if log_table %}Log table used in examples: `{{ log_table }}`{% endif %}
{% if metric_name %}Metric used in examples: `{{ metric_name }}`{% endif %}

## Investigation Flow

1. Use `SHOW TABLES` and `describe_table` to identify the actual metric, log, and trace tables.
2. Keep all queries inside the same time window.
3. Pivot by `service_name`, `trace_id`, `span_id`, route, status code, pod, host, or other shared dimensions.
4. Prefer narrow exploratory queries before expensive scans.

## Trace Pivot

```sql
-- Minimal trace pivot. Use trace_analysis for trace-only timeline and latency analysis.
SELECT timestamp, trace_id, span_id, parent_span_id, service_name, span_name, span_status_code
FROM {{ traces }}
WHERE timestamp >= '{{ start_time }}' AND timestamp < '{{ end_time }}'
{% if trace_id %}  AND trace_id = '{{ trace_id }}'
{% endif %}{% if service_name %}  AND service_name = '{{ service_name }}'
{% endif %}
ORDER BY timestamp LIMIT 100;
```

{% if log_table %}
## Log Pivot

```sql
-- Logs for a trace or service. Adjust column names after describe_table.
SELECT ts, service, level, message, trace_id
FROM {{ log_table }}
WHERE ts >= '{{ start_time }}' AND ts < '{{ end_time }}'
{% if trace_id %}  AND trace_id = '{{ trace_id }}'
{% endif %}{% if service_name %}  AND service = '{{ service_name }}'
{% endif %}
ORDER BY ts DESC LIMIT 100;

-- Error terms in the same window.
SELECT ts, service, level, message
FROM {{ log_table }}
WHERE ts >= '{{ start_time }}' AND ts < '{{ end_time }}'
  AND (message @@ 'error' OR message @@ 'exception' OR message @@ 'timeout')
{% if service_name %}  AND service = '{{ service_name }}'
{% endif %}
ORDER BY ts DESC LIMIT 100;
```
{% endif %}

{% if metric_name %}
## Metric Pivot

Use `execute_tql` for PromQL-compatible metrics. Pass only the PromQL expression
as `query`; pass `start`, `end`, and `step` separately.

```promql
-- Request rate by service.
sum by (service) (rate({{ metric_name }}[5m]))

-- If this metric is a histogram base name, use its _bucket series.
histogram_quantile(0.99, sum by (service, le) (rate({{ metric_name }}_bucket[5m])))
```
{% endif %}

## Output

- State the likely failing layer: metric anomaly, log error pattern, or trace bottleneck.
- Show the pivot path used and the key evidence.
- Suggest the next query, not a broad scan.

## References

- [SQL Query](https://docs.greptime.com/user-guide/query-data/sql)
- [TQL Reference](https://docs.greptime.com/reference/sql/tql)
- [Full-Text Search](https://docs.greptime.com/user-guide/logs/fulltext-search)
- [Traces Data Model](https://docs.greptime.com/user-guide/traces/data-model)
