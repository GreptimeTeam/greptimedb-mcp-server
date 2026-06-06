# Log Analysis: {{ table }}

{% set term = search_term | default("error", true) %}
{% if search_term %}
Search term: `{{ search_term }}`
{% else %}
No search term was provided. The examples below use `error`; replace it with the term or phrase you need.
{% endif %}

Start by checking the schema with `describe_table`. Do not assume the time
index, level, service, or message column names before looking at the table.

## Full-Text Search

```sql
-- Search logs containing a term or phrase. This is exact term matching and
-- works best when the target column has a FULLTEXT INDEX.
SELECT ts, level, message
FROM {{ table }}
WHERE message @@ '{{ term }}'
ORDER BY ts DESC LIMIT 100;

-- Equivalent function syntax.
SELECT ts, level, message
FROM {{ table }}
WHERE matches_term(message, '{{ term }}')
ORDER BY ts DESC LIMIT 50;

-- Case-insensitive search.
SELECT ts, level, message
FROM {{ table }}
WHERE lower(message) @@ lower('{{ term }}')
ORDER BY ts DESC LIMIT 50;
```

## Log Aggregation

```sql
-- Count by severity level
SELECT level, COUNT(*) as count
FROM {{ table }}
WHERE ts > now() - interval '1' hour
GROUP BY level ORDER BY count DESC;

-- Error rate over time (5-min buckets)
SELECT date_bin('5 minutes'::INTERVAL, ts) as bucket,
       COUNT(*) as total,
       SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors
FROM {{ table }}
WHERE ts > now() - interval '1' hour
GROUP BY bucket ORDER BY bucket;
```

## Recent Errors

```sql
-- Latest errors with context
SELECT ts, service, message
FROM {{ table }}
WHERE level IN ('ERROR', 'FATAL')
  AND ts > now() - interval '15' minute
ORDER BY ts DESC LIMIT 50;
```

## Notes

- Use `column @@ 'term'` or `matches_term(column, 'term')` for full-text search
- `matches_term` is exact term or phrase matching. It is case-sensitive unless you normalize with `lower()`
- Prefer a FULLTEXT INDEX on large text columns used for search
- Common log columns are often `ts`, `level`, `service`, `message`, and `trace_id`, but always verify the actual schema

## References

- [Full-Text Search](https://docs.greptime.com/user-guide/logs/fulltext-search) - Full-text search syntax and operators
- [Log Query](https://docs.greptime.com/user-guide/query-data/log-query) - Log query patterns and examples
- [SQL SELECT](https://docs.greptime.com/reference/sql/select) - SQL SELECT syntax reference
- [SQL Functions](https://docs.greptime.com/reference/sql/functions/overview) - Available SQL functions
