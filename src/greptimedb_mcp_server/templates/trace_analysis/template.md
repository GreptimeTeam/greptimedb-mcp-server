# Trace Analysis: {{ table }}

{% if trace_id %}Trace: {{ trace_id }}{% endif %}
{% if service_name %}Service: {{ service_name }}{% endif %}

## Common Span Columns

- `trace_id`, `span_id`, `parent_span_id` - Trace correlation
- `service_name`, `span_name` - Service and operation
- `duration_nano` - Span duration in nanoseconds
- `span_status_code` - STATUS_CODE_OK, STATUS_CODE_ERROR, STATUS_CODE_UNSET
- `timestamp` - Time index column

## Trace Lookup

```sql
{% if trace_id %}
-- Full trace timeline
SELECT span_name, service_name, duration_nano/1000000 as duration_ms,
       span_status_code, parent_span_id
FROM {{ table }}
WHERE trace_id = '{{ trace_id }}'
ORDER BY timestamp;
{% else %}
-- Recent traces
SELECT DISTINCT trace_id, MIN(timestamp) as start_time
FROM {{ table }}
WHERE timestamp > now() - INTERVAL '15 minutes'
GROUP BY trace_id
ORDER BY start_time DESC LIMIT 20;
{% endif %}
```

## Slow Spans

```sql
-- Top 10 slowest spans
SELECT trace_id, span_name, service_name,
       duration_nano/1000000 as duration_ms
FROM {{ table }}
WHERE timestamp > now() - INTERVAL '1 hour'
ORDER BY duration_nano DESC LIMIT 10;

-- p99 latency by service (using approx_percentile_cont)
SELECT service_name,
       approx_percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_nano/1000000) as p99_ms
FROM {{ table }}
WHERE timestamp > now() - INTERVAL '1 hour'
GROUP BY service_name ORDER BY p99_ms DESC;
```

## Error Analysis

```sql
-- Error spans
SELECT trace_id, service_name, span_name, span_status_code
FROM {{ table }}
WHERE span_status_code = 'STATUS_CODE_ERROR'
  AND timestamp > now() - INTERVAL '1 hour'
ORDER BY timestamp DESC LIMIT 50;

-- Error rate by service
SELECT service_name,
       COUNT(*) as total,
       SUM(CASE WHEN span_status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) as errors,
       100.0 * SUM(CASE WHEN span_status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) / COUNT(*) as error_pct
FROM {{ table }}
WHERE timestamp > now() - INTERVAL '1 hour'
GROUP BY service_name ORDER BY error_pct DESC;
```

## References

- [Traces Overview](https://docs.greptime.com/user-guide/traces/overview) - Trace data model and concepts
- [Traces Data Model](https://docs.greptime.com/user-guide/traces/data-model) - Span schema and OpenTelemetry mapping
- [Read and Write Traces](https://docs.greptime.com/user-guide/traces/read-write) - Query trace data
- [Jaeger Query](https://docs.greptime.com/user-guide/query-data/jaeger) - Jaeger-compatible trace queries
- [Approximate Functions](https://docs.greptime.com/reference/sql/functions/approximate) - approx_percentile_cont for latency percentiles
