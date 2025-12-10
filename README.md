# greptimedb-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/greptimedb-mcp-server)](https://pypi.org/project/greptimedb-mcp-server/)
![build workflow](https://github.com/GreptimeTeam/greptimedb-mcp-server/actions/workflows/python-app.yml/badge.svg)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

A Model Context Protocol (MCP) server for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) — an open-source, cloud-native, unified observability database.

This server enables AI assistants to query and analyze GreptimeDB through SQL, TQL (PromQL-compatible), and RANGE queries. It includes security features like read-only enforcement and sensitive data masking, along with prompt templates for common analysis tasks.

# Features

## Resources
- **list_resources** - List all tables in the database as browsable resources
- **read_resource** - Read table data via `greptime://<table>/data` URIs

## Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL queries with format (csv/json/markdown) and limit options |
| `describe_table` | Get table schema including column names, types, and constraints |
| `health_check` | Check database connection status and server version |
| `execute_tql` | Execute TQL (PromQL-compatible) queries for time-series analysis |
| `query_range` | Execute time-window aggregation queries with RANGE/ALIGN syntax |
| `explain_query` | Analyze SQL or TQL query execution plans |
| `list_pipelines` | List all pipelines or get details of a specific pipeline |
| `create_pipeline` | Create a new pipeline with YAML configuration |
| `dryrun_pipeline` | Test a pipeline with sample data without writing to database |
| `delete_pipeline` | Delete a specific version of a pipeline |

## Prompts

MCP prompt system APIs:
- **list_prompts** - List available prompt templates
- **get_prompt** - Get a prompt template by name with argument substitution

Available prompt templates:

| Prompt | Description |
|--------|-------------|
| `pipeline_creator` | Generate GreptimeDB pipeline YAML configuration from log samples |
| `log_pipeline` | Log analysis with full-text search and aggregation |
| `metrics_analysis` | Comprehensive metrics analysis for monitoring data |
| `promql_analysis` | PromQL-style queries using GreptimeDB TQL EVAL syntax |
| `iot_monitoring` | IoT device monitoring with TAG/FIELD semantics and device aggregation |
| `trace_analysis` | Distributed trace analysis for OpenTelemetry spans |
| `table_operation` | Table diagnostics: schema, region health, storage analysis, and query optimization |

### Using Prompts in Claude Desktop

In Claude Desktop, MCP prompts need to be added manually to your conversation:

1. Click the **+** button in the conversation input area
2. Select **MCP Server**
3. Choose **Prompt/References**
4. Select the prompt you want to use (e.g., `pipeline_creator`)
5. Fill in the required arguments

Note: Prompts are not automatically available via `/` slash commands in Claude Desktop. You must add them through the UI as described above.

### LLM Instructions

Add this to your system prompt or custom instructions to help AI assistants use this MCP server effectively:

```
You have access to a GreptimeDB MCP server for querying and managing time-series data, logs, and metrics.

## Available Tools
- `execute_sql`: Run SQL queries (SELECT, SHOW, DESCRIBE only - read-only access)
- `execute_tql`: Run PromQL-compatible time-series queries
- `query_range`: Time-window aggregation with RANGE/ALIGN syntax
- `describe_table`: Get table schema information
- `health_check`: Check database connection status
- `explain_query`: Analyze query execution plans

### Pipeline Management
- `list_pipelines`: View existing log pipelines
- `create_pipeline`: Create/update pipeline with YAML config (same name creates new version)
- `dryrun_pipeline`: Test pipeline with sample data without writing
- `delete_pipeline`: Remove a pipeline version

**Note**: All HTTP API calls (pipeline tools) require authentication. The MCP server handles auth automatically using configured credentials. When providing curl examples to users, always include `-u <username>:<password>`.

## Available Prompts
Use these prompts for specialized tasks:
- `pipeline_creator`: Generate pipeline YAML from log samples - use when user provides log examples
- `log_pipeline`: Log analysis with full-text search
- `metrics_analysis`: Metrics monitoring and analysis
- `promql_analysis`: PromQL-style queries
- `iot_monitoring`: IoT device data analysis
- `trace_analysis`: Distributed tracing analysis
- `table_operation`: Table diagnostics and optimization

## Workflow Tips
1. For log pipeline creation: Get log sample → use `pipeline_creator` prompt → generate YAML → `create_pipeline` → `dryrun_pipeline` to verify
2. For data analysis: `describe_table` first → understand schema → `execute_sql` or `execute_tql`
3. For time-series: Prefer `query_range` for aggregations, `execute_tql` for PromQL patterns
4. Always check `health_check` if queries fail unexpectedly
```

### Example: Creating a Pipeline

Ask Claude to help create a pipeline by providing your log sample:

```
Help me create a GreptimeDB pipeline to parse this nginx log:
127.0.0.1 - - [25/May/2024:20:16:37 +0000] "GET /index.html HTTP/1.1" 200 612 "-" "Mozilla/5.0..."
```

Claude will:
1. Analyze your log format
2. Generate a pipeline YAML configuration
3. Create the pipeline using `create_pipeline` tool
4. Test it with `dryrun_pipeline` tool

## Security

### Database User Configuration (Recommended)

For production deployments, create a **read-only database user** for the MCP server. This provides defense-in-depth security at the database level.

Configure a read-only user in GreptimeDB using [static user provider](https://docs.greptime.com/user-guide/deployments-administration/authentication/static/#permission-modes):

```
# User format: username:permission_mode=password
mcp_readonly:readonly=your_secure_password
```

Permission modes:
- `readonly` (or `ro`) - Can only query data (recommended for MCP server)
- `writeonly` (or `wo`) - Can only write data
- `readwrite` (or `rw`) - Full access (default)

Then configure the MCP server to use this user:
```bash
GREPTIMEDB_USER=mcp_readonly
GREPTIMEDB_PASSWORD=your_secure_password
```

### Application-Level Security Gate

All queries also pass through a security gate that:
- Blocks DDL/DML operations: DROP, DELETE, TRUNCATE, UPDATE, INSERT, ALTER, CREATE, GRANT, REVOKE
- Blocks dynamic SQL execution: EXEC, EXECUTE, CALL
- Blocks data modification: REPLACE INTO
- Blocks file system access: LOAD, COPY, OUTFILE, LOAD_FILE, INTO DUMPFILE
- Blocks encoded content bypass attempts: hex encoding (0x...), UNHEX(), CHAR()
- Prevents multiple statement execution with dangerous operations
- Allows read-only operations: SELECT, SHOW, DESCRIBE, TQL, EXPLAIN, UNION, INFORMATION_SCHEMA

### Audit Logging

All tool invocations are logged for security auditing and compliance. Each log entry includes:
- Tool name and parameters (truncated to 200 characters)
- Execution status (success/failure)
- Duration in milliseconds
- Error details (if any)

**Log format:**
```
2024-01-15 10:30:45 - greptimedb_mcp_server.audit - INFO - [AUDIT] execute_sql | query="SELECT * FROM cpu LIMIT 10" | success=True | duration_ms=45.2
```

**Configuration:**
```bash
# Disable audit logging (default: true)
GREPTIMEDB_AUDIT_ENABLED=false

# Or via CLI
greptimedb-mcp-server --audit-enabled false
```

## Data Masking
Sensitive data in query results is automatically masked to protect privacy:

**Default masked column patterns:**
- Authentication: `password`, `passwd`, `pwd`, `secret`, `token`, `api_key`, `access_key`, `private_key`, `credential`, `auth`
- Financial: `credit_card`, `card_number`, `cvv`, `cvc`, `pin`, `bank_account`, `account_number`, `iban`, `swift`
- Personal: `ssn`, `social_security`, `id_card`, `passport`

**Configuration:**
```bash
# Disable masking (default: true)
GREPTIMEDB_MASK_ENABLED=false

# Add custom patterns (comma-separated)
GREPTIMEDB_MASK_PATTERNS=phone,address,email
```

Masked values appear as `******` in all output formats (CSV, JSON, Markdown).

# Installation

```bash
pip install greptimedb-mcp-server

# Upgrade to latest version
pip install -U greptimedb-mcp-server
```

After installation, run the server:

```bash
# Using the command
greptimedb-mcp-server --host localhost --port 4002 --database public

# Or as a Python module
python -m greptimedb_mcp_server.server
```

# Configuration

Set the following environment variables:

```bash
GREPTIMEDB_HOST=localhost      # Database host
GREPTIMEDB_PORT=4002           # Optional: Database MySQL port (defaults to 4002)
GREPTIMEDB_HTTP_PORT=4000      # Optional: HTTP API port for pipeline management (defaults to 4000)
GREPTIMEDB_HTTP_PROTOCOL=http  # Optional: HTTP protocol (http or https, defaults to http)
GREPTIMEDB_USER=root
GREPTIMEDB_PASSWORD=
GREPTIMEDB_DATABASE=public
GREPTIMEDB_TIMEZONE=UTC
GREPTIMEDB_POOL_SIZE=5         # Optional: Connection pool size (defaults to 5)
GREPTIMEDB_MASK_ENABLED=true   # Optional: Enable data masking (defaults to true)
GREPTIMEDB_MASK_PATTERNS=      # Optional: Additional sensitive column patterns (comma-separated)

# MCP Server Transport Options
GREPTIMEDB_TRANSPORT=stdio     # Optional: Transport mode (stdio, sse, streamable-http, defaults to stdio)
GREPTIMEDB_LISTEN_HOST=0.0.0.0 # Optional: HTTP server bind host (defaults to 0.0.0.0)
GREPTIMEDB_LISTEN_PORT=8080    # Optional: HTTP server bind port (defaults to 8080)
```

Or via command-line args:

* `--host` the database host, `localhost` by default,
* `--port` the database port, must be MySQL protocol port,  `4002` by default,
* `--http-port` the HTTP API port for pipeline management, `4000` by default,
* `--http-protocol` the HTTP protocol for API calls (http or https), `http` by default,
* `--user` the database username, empty by default,
* `--password` the database password, empty by default,
* `--database` the database name, `public` by default,
* `--timezone` the session time zone, empty by default (using server default time zone),
* `--pool-size` the connection pool size, `5` by default,
* `--mask-enabled` enable data masking for sensitive columns, `true` by default,
* `--mask-patterns` additional sensitive column patterns (comma-separated), empty by default,
* `--transport` MCP transport mode (`stdio`, `sse`, `streamable-http`), `stdio` by default,
* `--listen-host` HTTP server bind host (for sse/streamable-http), `0.0.0.0` by default,
* `--listen-port` HTTP server bind port (for sse/streamable-http), `8080` by default.

## HTTP Server Mode

For containerized or Kubernetes deployments, you can run the MCP server in HTTP mode instead of stdio:

```bash
# Streamable HTTP mode (recommended for production)
greptimedb-mcp-server --transport streamable-http --listen-port 8080

# SSE mode (legacy, for older clients)
greptimedb-mcp-server --transport sse --listen-host 0.0.0.0 --listen-port 3000

# Via environment variables (for Docker/K8s)
GREPTIMEDB_TRANSPORT=streamable-http \
GREPTIMEDB_LISTEN_HOST=0.0.0.0 \
GREPTIMEDB_LISTEN_PORT=8080 \
greptimedb-mcp-server
```

**Transport modes:**
- `stdio` (default): Standard input/output, for local CLI integration (e.g., Claude Desktop)
- `streamable-http`: HTTP-based transport with SSE streaming, recommended for remote/production deployments
- `sse`: Server-Sent Events transport (legacy, being deprecated in MCP spec)

# Usage

## Tool Examples

### execute_sql
Execute SQL queries with optional format and limit:
```json
{
  "query": "SELECT * FROM metrics WHERE host = 'server1'",
  "format": "json",
  "limit": 100
}
```
Formats: `csv` (default), `json`, `markdown`

### execute_tql
Execute PromQL-compatible time-series queries:
```json
{
  "query": "rate(http_requests_total[5m])",
  "start": "2024-01-01T00:00:00Z",
  "end": "2024-01-01T01:00:00Z",
  "step": "1m",
  "lookback": "5m"
}
```

### query_range
Execute time-window aggregation queries:
```json
{
  "table": "metrics",
  "select": "ts, host, avg(cpu) RANGE '5m'",
  "align": "1m",
  "by": "host",
  "where": "region = 'us-east'"
}
```

### describe_table
Get table schema information:
```json
{
  "table": "metrics"
}
```

### explain_query
Analyze query execution plan:
```json
{
  "query": "SELECT * FROM metrics",
  "analyze": true
}
```

### health_check
Check database connection (no parameters required).

### Pipeline Management

#### list_pipelines
List all pipelines or filter by name:
```json
{
  "name": "my_pipeline"
}
```

#### create_pipeline
Create a new pipeline with YAML configuration:
```json
{
  "name": "nginx_logs",
  "pipeline": "version: 2\nprocessors:\n  - dissect:\n      fields:\n        - message\n      patterns:\n        - '%{ip} - - [%{timestamp}] \"%{method} %{path}\"'\n      ignore_missing: true\n  - date:\n      fields:\n        - timestamp\n      formats:\n        - '%d/%b/%Y:%H:%M:%S %z'\n\ntransform:\n  - fields:\n      - ip\n    type: string\n    index: inverted\n  - fields:\n      - timestamp\n    type: time\n    index: timestamp"
}
```

#### dryrun_pipeline
Test a pipeline with sample data (no actual write):
```json
{
  "pipeline_name": "nginx_logs",
  "data": "{\"message\": \"127.0.0.1 - - [25/May/2024:20:16:37 +0000] \\\"GET /index.html\\\"\"}"
}
```

#### delete_pipeline
Delete a specific version of a pipeline:
```json
{
  "name": "nginx_logs",
  "version": "2024-06-27 12:02:34.257312110"
}
```

## Claude Desktop Integration

Configure the MCP server in Claude Desktop's configuration file:

#### MacOS

Location: `~/Library/Application Support/Claude/claude_desktop_config.json`

#### Windows

Location: `%APPDATA%/Claude/claude_desktop_config.json`

**Option 1: Using pip installed command (recommended)**

```json
{
  "mcpServers": {
    "greptimedb": {
      "command": "greptimedb-mcp-server",
      "args": [
        "--host", "localhost",
        "--port", "4002",
        "--database", "public"
      ]
    }
  }
}
```

**Option 2: Using uv with source directory**

```json
{
  "mcpServers": {
    "greptimedb": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/greptimedb-mcp-server",
        "run",
        "-m",
        "greptimedb_mcp_server.server"
      ],
      "env": {
        "GREPTIMEDB_HOST": "localhost",
        "GREPTIMEDB_PORT": "4002",
        "GREPTIMEDB_USER": "root",
        "GREPTIMEDB_PASSWORD": "",
        "GREPTIMEDB_DATABASE": "public",
        "GREPTIMEDB_TIMEZONE": "",
        "GREPTIMEDB_POOL_SIZE": "5",
        "GREPTIMEDB_HTTP_PORT": "4000",
        "GREPTIMEDB_MASK_ENABLED": "true",
        "GREPTIMEDB_MASK_PATTERNS": ""
      }
    }
  }
}
```

# License

MIT License - see LICENSE.md file for details.

# Contribute

## Prerequisites
- Python with `uv` package manager
- GreptimeDB installation
- MCP server dependencies

## Development

```
# Clone the repository
git clone https://github.com/GreptimeTeam/greptimedb-mcp-server.git
cd greptimedb-mcp-server

# Create virtual environment
uv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
uv sync

# Run tests
pytest
```

Use [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) for debugging:

```bash
npx @modelcontextprotocol/inspector uv \
  --directory \
  /path/to/greptimedb-mcp-server \
  run \
  -m \
  greptimedb_mcp_server.server
```

# Acknowledgement
This library's implementation was inspired by the following two repositories and incorporates their code, for which we express our gratitude：

* [ktanaka101/mcp-server-duckdb](https://github.com/ktanaka101/mcp-server-duckdb)
* [designcomputer/mysql_mcp_server](https://github.com/designcomputer/mysql_mcp_server)
* [mikeskarl/mcp-prompt-templates](https://github.com/mikeskarl/mcp-prompt-templates)

Thanks!
