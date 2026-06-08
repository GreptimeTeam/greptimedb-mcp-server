# Pipeline Generator for GreptimeDB

Generate a GreptimeDB pipeline configuration based on the provided log sample.

For complete pipeline syntax and documentation, refer to: https://docs.greptime.com/reference/pipeline/pipeline-config/

## Pipeline Name
{{ pipeline_name }}

## Log Sample
```
{{ log_sample }}
```

## Task

Analyze the log sample above and generate a GreptimeDB pipeline YAML configuration that:
1. Parses the log format correctly
2. Extracts meaningful fields
3. Sets appropriate data types and indexes

## Pipeline Configuration Guidelines

### Version
Use `version: 2` for current pipeline configurations.

### Processors (choose appropriate ones)

**dissect** - Split log by delimiters (fast, for structured logs):
```yaml
- dissect:
    fields:
      - message
    patterns:
      - '%{field1} %{field2} [%{timestamp}]'
    ignore_missing: true
```

**regex** - Extract fields with regular expressions (flexible, for complex patterns):
```yaml
- regex:
    fields:
      - message
    patterns:
      - '(?<ip>\d+\.\d+\.\d+\.\d+).*\[(?<time>[^\]]+)\]'
    ignore_missing: true
```

**vrl** - Use VRL for nested JSON, conditional logic, or normalization that is hard
to express with simple processors:
```yaml
- vrl:
    source: |
      .level = upcase(string!(.level))
      .message_length = length(string!(.message))
```

**date** - Parse formatted time strings:
```yaml
- date:
    fields:
      - timestamp
    formats:
      - '%Y-%m-%d %H:%M:%S%.3f'
      - '%d/%b/%Y:%H:%M:%S %z'
    timezone: 'UTC'
    ignore_missing: true
```

**epoch** - Parse numeric timestamps:
```yaml
- epoch:
    fields:
      - timestamp
    resolution: millisecond  # or second, microsecond, nanosecond
    ignore_missing: true
```

**gsub** - Replace strings:
```yaml
- gsub:
    fields:
      - message
    pattern: 'old'
    replacement: 'new'
    ignore_missing: true
```

**select** - Keep or exclude fields:
```yaml
- select:
    type: exclude  # or include
    fields:
      - message  # remove original message after parsing
```

### Transform (define schema)

```yaml
transform:
  - fields:
      - ip_address
    type: string
    index: inverted  # for equality queries
  - fields:
      - request_line
    type: string
    index: fulltext  # for text search
  - fields:
      - status_code
    type: int32
    index: inverted
  - fields:
      - response_size
    type: int64
  - fields:
      - request_id
    type: string
    index: skipping  # for high-cardinality IDs
  - fields:
      - timestamp
    type: time
    index: timestamp  # required: exactly one timestamp field
```

### Data Types
- `string`: Text data
- `int8`, `int16`, `int32`, `int64`: Integers
- `uint8`, `uint16`, `uint32`, `uint64`: Unsigned integers
- `float32`, `float64`: Floating point
- `time`: Parsed timestamp (from date/epoch processor)
- `epoch, s|ms|us|ns`: Raw epoch timestamp with precision. Use this when the
  source field should be converted directly in `transform`

### Index Types
- `timestamp`: Time index column (required, exactly one)
- `inverted`: For equality/range queries on low-cardinality fields
- `fulltext`: For text search on log messages
- `skipping`: For high-cardinality string fields

## Output

Generate a complete, valid YAML pipeline configuration. After generation:
1. Use `dryrun_pipeline` with inline pipeline YAML and representative sample data
2. Only call `create_pipeline` after dry-run succeeds and the generated schema looks right

**Note**: You can update an existing pipeline by calling `create_pipeline` with the same name. Each call creates a new version. Use `list_pipelines` to view all versions, and `delete_pipeline` to remove specific versions.

## Testing with dryrun_pipeline

Use `dryrun_pipeline` with separated parameters:

**Example with inline pipeline YAML:**
```python
dryrun_pipeline(
    pipeline='''version: 2
processors:
  - date:
      fields:
        - timestamp
      formats:
        - '%Y-%m-%dT%H:%M:%SZ' ''',
    data='{"timestamp": "2024-05-25T20:16:37Z", "level": "INFO"}',
    data_type='application/json'
)
```

**Example with saved pipeline:**
```python
dryrun_pipeline(
    pipeline_name='my_log_pipeline',
    data='{"message": "127.0.0.1 - - [25/May/2024:20:16:37 +0000]"}',
    data_type='application/x-ndjson'
)
```

**Data Formats:**
- **Single log entry:** `{"message": "127.0.0.1 - - [25/May/2024:20:16:37 +0000]"}`
- **Multiple entries (JSON array):** `[{"message": "log1"}, {"message": "log2"}]`
- **NDJSON (newline-delimited):** Use `data_type='application/x-ndjson'` with data like `{"msg":"line1"}\n{"msg":"line2"}`
- **Plain text:** Use `data_type='text/plain'`; each line is available as the `message` field

## Common Log Format Examples

**Nginx/Apache Access Log:**
```
127.0.0.1 - - [25/May/2024:20:16:37 +0000] "GET /index.html HTTP/1.1" 200 612 "-" "Mozilla/5.0..."
```
Pattern: `%{ip} - - [%{timestamp}] "%{method} %{path} %{protocol}" %{status} %{size} "-" "%{user_agent}"`

**JSON Structured Log:**
```json
{"timestamp": "2024-05-25T20:16:37Z", "level": "INFO", "service": "api", "message": "Request processed", "duration_ms": 42}
```
No dissect needed - fields map directly.

**Syslog Format:**
```
May 25 20:16:37 hostname app[1234]: Connection established from 192.168.1.1
```
Pattern: `%{timestamp} %{hostname} %{app}[%{pid}]: %{message}`

## Pipeline Scope

- Focus on parsing, type conversion, timestamp extraction, and `transform` output.
- Keep the original `message` field when operators need raw log context or full-text search. Exclude it only when parsed fields are enough.
- Ensure exactly one output field has `index: timestamp`.
- Use `schema_design_advisor` for primary key, partitioning, retention, and broader table-design decisions.

## Troubleshooting

If `dryrun_pipeline` fails:
- **Missing required parameters**: Ensure you provide `data` and exactly one of `pipeline` or `pipeline_name`
- **Both pipeline and pipeline_name provided**: Only provide one of them
- **Pattern mismatch**: Check if dissect/regex pattern matches the log format exactly
- **Date format error**: Verify the date format string matches the timestamp in logs
- **Missing fields**: Use `ignore_missing: true` in processors to handle optional fields
- **Type conversion**: Ensure numeric fields (status_code, size) are converted to appropriate int types
- **HTTP 401/403 errors**: Check whether GreptimeDB authentication is enabled and whether MCP credentials are configured

## HTTP API Authentication

The MCP tools use the configured HTTP credentials automatically. For manual
curl examples, include `-u "<username>:<password>"` when authentication is
enabled; omit it for unauthenticated local deployments.

```bash
# Create pipeline
curl -X POST "http://localhost:4000/v1/pipelines/my_pipeline" \
  -H "Content-Type: application/x-yaml" \
  -d @pipeline.yaml

# Dryrun pipeline (constructs JSON request internally)
curl -X POST "http://localhost:4000/v1/pipelines/_dryrun" \
  -H "Content-Type: application/json" \
  -d '{"pipeline": "version: 2", "data": "{\"timestamp\": \"2024-05-25T20:16:37Z\"}", "data_type": "application/json"}'

# Delete pipeline
curl -X DELETE "http://localhost:4000/v1/pipelines/my_pipeline?version=<version>" \
  -u "<username>:<password>"  # only when auth is enabled
```

The MCP server tools (`create_pipeline`, `dryrun_pipeline`, `delete_pipeline`) handle authentication automatically using configured credentials.

## Example Output Format

```yaml
version: 2
processors:
  - dissect:
      fields:
        - message
      patterns:
        - '%{ip} - - [%{timestamp}] "%{method} %{path} %{protocol}" %{status} %{size}'
      ignore_missing: true
  - date:
      fields:
        - timestamp
      formats:
        - '%d/%b/%Y:%H:%M:%S %z'
  - select:
      type: exclude
      fields:
        - message

transform:
  - fields:
      - ip
    type: string
    index: inverted   # low-cardinality, for filtering
  - fields:
      - method
    type: string
    index: inverted   # low-cardinality (GET, POST, etc.)
  - fields:
      - path
    type: string
    index: fulltext   # for text search on URL paths
  - fields:
      - protocol
    type: string
  - fields:
      - status
    type: int32
    index: inverted   # for filtering by status code
  - fields:
      - size
    type: int64
  - fields:
      - timestamp
    type: time
    index: timestamp
```

## References

- [Pipeline Configuration Reference](https://docs.greptime.com/reference/pipeline/pipeline-config/) - Complete pipeline syntax and processors
- [Manage Pipelines](https://docs.greptime.com/user-guide/logs/manage-pipelines) - Create, update, and delete pipelines
- [Data Index](https://docs.greptime.com/user-guide/manage-data/data-index) - Index types and selection guide
- [Table Design Best Practices](https://docs.greptime.com/user-guide/deployments-administration/performance-tuning/design-table/) - Tag and index selection
- [Logs Overview](https://docs.greptime.com/user-guide/logs/overview) - Log data model and concepts
