# IoT Monitoring: {{ table }}

{% if device_id %}Device: {{ device_id }}{% endif %}

## Data Model

- **TAG columns**: Device identifiers (device_id, region, type) - indexed for fast filtering
- **FIELD columns**: Sensor values (temperature, humidity, voltage) - numeric measurements
- **TIME INDEX**: Timestamp column (typically `ts`)

## Device Overview

```sql
-- List all devices
SELECT DISTINCT device_id, region, type
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 day';

-- Last report time per device
SELECT device_id, MAX(ts) as last_seen
FROM {{ table }}
GROUP BY device_id
ORDER BY last_seen DESC;
```

## Device Metrics

```sql
{% if device_id %}
-- Single device stats
SELECT
    MIN(value) as min_val,
    AVG(value) as avg_val,
    MAX(value) as max_val,
    STDDEV(value) as stddev_val
FROM {{ table }}
WHERE device_id = '{{ device_id }}'
  AND ts > now() - INTERVAL '1 hour';
{% else %}
-- Per-device aggregation
SELECT device_id,
       AVG(value) as avg_val,
       MAX(value) as max_val
FROM {{ table }}
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY device_id;
{% endif %}
```

## Anomaly Detection

```sql
-- Devices outside 2 standard deviations
WITH stats AS (
    SELECT device_id, AVG(value) as m, STDDEV(value) as s
    FROM {{ table }}
    WHERE ts > now() - INTERVAL '1 hour'
    GROUP BY device_id
)
SELECT t.device_id, t.ts, t.value
FROM {{ table }} t JOIN stats s ON t.device_id = s.device_id
WHERE t.ts > now() - INTERVAL '10 minutes'
  AND (t.value > s.m + 2*s.s OR t.value < s.m - 2*s.s);
```

## Offline Devices

```sql
-- Devices not reporting in last 5 minutes
SELECT device_id, MAX(ts) as last_seen
FROM {{ table }}
GROUP BY device_id
HAVING MAX(ts) < now() - INTERVAL '5 minutes';
```

## References

- [Data Model](https://docs.greptime.com/user-guide/concepts/data-model) - Tag, Field, and Timestamp concepts
- [CREATE TABLE](https://docs.greptime.com/reference/sql/create) - Table creation with TAG/FIELD semantics
- [RANGE Query](https://docs.greptime.com/reference/sql/range) - Time-window aggregation for sensor data
- [SQL Functions](https://docs.greptime.com/reference/sql/functions/overview) - Aggregation functions (AVG, MAX, STDDEV)
- [Table Design Best Practices](https://docs.greptime.com/user-guide/deployments-administration/performance-tuning/design-table/) - Optimal schema design for IoT
