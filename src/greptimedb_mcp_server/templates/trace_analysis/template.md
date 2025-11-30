# Trace Analysis: {{ table }}

{% if trace_id %}Trace: {{ trace_id }}{% endif %}
{% if service_name %}Service: {{ service_name }}{% endif %}

## Common Span Columns

- `trace_id`, `span_id`, `parent_span_id` - Trace correlation
- `service_name`, `span_name` - Service and operation
- `duration_nano` - Span duration in nanoseconds
- `status_code` - OK, ERROR, UNSET

## Trace Lookup

```sql
{% if trace_id %}
-- Full trace timeline
SELECT span_name, service_name, duration_nano/1000000 as duration_ms,
       status_code, parent_span_id
FROM {{ table }}
WHERE trace_id = '{{ trace_id }}'
ORDER BY ts;
{% else %}
-- Recent traces
SELECT DISTINCT trace_id, MIN(ts) as start_time
FROM {{ table }}
WHERE ts > now() - INTERVAL '15 minutes'
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
WHERE ts > now() - INTERVAL '1 hour'
ORDER BY duration_nano DESC LIMIT 10;

-- p99 latency by service
SELECT service_name,
       APPROX_PERCENTILE_CONT(duration_nano/1000000, 0.99) as p99_ms
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY service_name ORDER BY p99_ms DESC;
```

## Error Analysis

```sql
-- Error spans
SELECT trace_id, service_name, span_name, status_code
FROM {{ table }}
WHERE status_code = 'ERROR'
  AND ts > now() - INTERVAL '1 hour'
ORDER BY ts DESC LIMIT 50;

-- Error rate by service
SELECT service_name,
       COUNT(*) as total,
       SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) as errors,
       100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*) as error_pct
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY service_name ORDER BY error_pct DESC;
```
