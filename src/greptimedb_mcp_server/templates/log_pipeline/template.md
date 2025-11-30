# Log Analysis: {{ table }}

{% if search_term %}Search: "{{ search_term }}"{% endif %}

## Full-Text Search

```sql
-- Search logs containing keyword
SELECT ts, level, message
FROM {{ table }}
WHERE MATCHES(message, '{{ search_term | default("error") }}')
ORDER BY ts DESC LIMIT 100;

-- Case-insensitive search
SELECT ts, level, message
FROM {{ table }}
WHERE MATCHES(message, '(?i){{ search_term | default("exception") }}')
ORDER BY ts DESC LIMIT 50;
```

## Log Aggregation

```sql
-- Count by severity level
SELECT level, COUNT(*) as count
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY level ORDER BY count DESC;

-- Error rate over time (5-min buckets)
SELECT date_bin(INTERVAL '5 minutes', ts) as bucket,
       COUNT(*) as total,
       SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket ORDER BY bucket;
```

## Recent Errors

```sql
-- Latest errors with context
SELECT ts, service, message
FROM {{ table }}
WHERE level IN ('ERROR', 'FATAL')
  AND ts > now() - INTERVAL '15 minutes'
ORDER BY ts DESC LIMIT 50;
```

## Notes

- Use `MATCHES(column, 'pattern')` for full-text search (requires FULLTEXT index)
- Common log columns: ts, level, service, message, trace_id
- Regex patterns supported in MATCHES function
