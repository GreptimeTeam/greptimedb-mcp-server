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
Always use `version: 2` for the latest pipeline format with auto-transform support.

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
- `epoch, s|ms|us|ns`: Raw epoch timestamp with precision

### Index Types
- `timestamp`: Time index column (required, exactly one)
- `inverted`: For equality/range queries on low-cardinality fields
- `fulltext`: For text search on log messages
- `skipping`: For high-cardinality string fields

## Output

Generate a complete, valid YAML pipeline configuration. After generation:
1. Use `create_pipeline` tool to create the pipeline
2. Use `dryrun_pipeline` tool to verify with sample data

**Note**: You can update an existing pipeline by calling `create_pipeline` with the same name. Each call creates a new version. Use `list_pipelines` to view all versions, and `delete_pipeline` to remove specific versions.

## Testing with dryrun_pipeline

The `dryrun_pipeline` tool accepts JSON data in the following formats:

**Single log entry (JSON object with "message" field for plain text logs):**
```json
{"message": "127.0.0.1 - - [25/May/2024:20:16:37 +0000] \"GET /index.html HTTP/1.1\" 200 612"}
```

**Multiple log entries (JSON array):**
```json
[
  {"message": "127.0.0.1 - - [25/May/2024:20:16:37 +0000] \"GET /index.html HTTP/1.1\" 200 612"},
  {"message": "192.168.1.1 - - [25/May/2024:20:17:37 +0000] \"POST /api/login HTTP/1.1\" 200 1784"}
]
```

**Structured JSON logs (fields map directly to pipeline input):**
```json
{"timestamp": "2024-05-25 20:16:37", "level": "INFO", "service": "api", "message": "Request processed"}
```

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

## Best Practices

### Tag Selection (Primary Key)
- **For log tables, avoid using tags (primary keys)** - GreptimeDB sorts data by (primary key, timestamp), and for logs, sorting by timestamp alone is usually sufficient
- If you must use tags, choose **low-cardinality fields** (< 10,000 unique values) like `service_name`, `region`, `log_level`
- **Do NOT use high-cardinality fields as tags** (e.g., `request_id`, `trace_id`, `user_id`)

### Index Selection
| Index Type | When to Use | Supported Operations | Storage Overhead |
|------------|-------------|---------------------|------------------|
| `inverted` | Low-cardinality fields for filtering | `=`, `!=`, `IN`, `BETWEEN`, `>`, `<` | Medium-High |
| `fulltext` | Unstructured text needing tokenized search | `MATCHES` (full-text search) | High |
| `skipping` | High-cardinality fields (e.g., `request_id`) | `=` only | Low |

**Guidelines:**
- Use `index: inverted` for fields like `status_code`, `method`, `log_level` - creates mapping between values and rows
- Use `index: fulltext` for log message body or unstructured text - supports word-based matching
- Use `index: skipping` for high-cardinality IDs like `request_id`, `trace_id`, `mac_address` - maintains min/max metadata per data block
- **Only index columns frequently used in WHERE clauses** - unnecessary indexes waste storage and slow down ingestion
- Fields without index can still be queried, just slower
- Inverted index becomes inefficient with too many unique values - consider skipping index instead

### Table Design for Logs
- **Append-only mode**: Log tables should use append mode (no updates/deletes)
- **No time-based partitioning needed**: GreptimeDB automatically partitions by TIME INDEX
- **Partition by business dimension** if needed (e.g., by datacenter or application)

### Other Tips
- After parsing, use `select` processor to exclude the original `message` field to save storage
- Always ensure exactly one field has `index: timestamp` - this is required
- Prefer structured logging over full-text search for better query performance

## Troubleshooting

If `dryrun_pipeline` fails:
- **Pattern mismatch**: Check if dissect/regex pattern matches the log format exactly
- **Date format error**: Verify the date format string matches the timestamp in logs
- **Missing fields**: Use `ignore_missing: true` in processors to handle optional fields
- **Type conversion**: Ensure numeric fields (status_code, size) are converted to appropriate int types
- **HTTP 401/403 errors**: Authentication is required for all HTTP API calls

## HTTP API Authentication

All GreptimeDB HTTP API calls require Basic Authentication. When providing curl examples for manual testing, always include the `-u` flag:

```bash
# Create pipeline
curl -X POST "http://localhost:4000/v1/pipelines/my_pipeline" \
  -u "<username>:<password>" \
  -H "Content-Type: application/x-yaml" \
  -d @pipeline.yaml

# Dryrun pipeline
curl -X POST "http://localhost:4000/v1/pipelines/dryrun?pipeline_name=my_pipeline" \
  -u "<username>:<password>" \
  -H "Content-Type: application/json" \
  -d '{"message": "test log entry"}'

# Delete pipeline
curl -X DELETE "http://localhost:4000/v1/pipelines/my_pipeline?version=<version>" \
  -u "<username>:<password>"
```

Replace `<username>` and `<password>` with actual credentials. The MCP server tools (`create_pipeline`, `dryrun_pipeline`, `delete_pipeline`) handle authentication automatically using configured credentials.

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
