# PromQL Analysis: {{ metric }}

**Time Range**: {{ start_time }} to {{ end_time }}

## TQL EVAL Syntax

Use `execute_tql` tool with PromQL expressions. Time parameters are passed separately.

## Common Queries

### Rate & Increase

```promql
-- Request rate (per second)
rate({{ metric }}[5m])

-- Total increase over window
increase({{ metric }}[1h])

-- Rate with label filter
rate({{ metric }}{job="api", status="500"}[5m])
```

### Aggregations

```promql
-- Sum across all instances
sum(rate({{ metric }}[5m]))

-- Sum by label
sum by (instance) (rate({{ metric }}[5m]))

-- Average by service
avg by (service) ({{ metric }})
```

### Histogram Percentiles

```promql
-- 99th percentile latency
histogram_quantile(0.99, rate({{ metric }}_bucket[5m]))

-- Multiple percentiles
histogram_quantile(0.95, sum by (le) (rate({{ metric }}_bucket[5m])))
```

### Comparison & Alerts

```promql
-- Error rate > 1%
sum(rate(errors_total[5m])) / sum(rate(requests_total[5m])) > 0.01

-- High latency detection
histogram_quantile(0.99, rate({{ metric }}_bucket[5m])) > 0.5
```

## Notes

- Use `execute_tql` tool with: query, start, end, step (required), lookback (optional)
- Time formats: SQL expression (now(), now() - interval '5' minute), RFC3339, or Unix timestamp
- Label matchers: `=`, `!=`, `=~` (regex), `!~`
- Time durations: s, m, h, d, w

## References

- [TQL (PromQL) Reference](https://docs.greptime.com/reference/sql/tql) - TQL EVAL syntax and supported functions
- [PromQL Query Guide](https://docs.greptime.com/user-guide/query-data/promql) - PromQL query patterns and examples
- [Time Durations](https://docs.greptime.com/reference/time-durations) - Time duration format reference
