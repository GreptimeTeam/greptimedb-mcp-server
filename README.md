# greptimedb-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/greptimedb-mcp-server)](https://pypi.org/project/greptimedb-mcp-server/)
![build workflow](https://github.com/GreptimeTeam/greptimedb-mcp-server/actions/workflows/python-app.yml/badge.svg)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

A Model Context Protocol (MCP) server for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) — an open-source, cloud-native, unified observability database.

Enables AI assistants to query and analyze GreptimeDB using SQL, TQL (PromQL-compatible), and RANGE queries, with built-in security features like read-only enforcement and data masking.

## Quick Start

```bash
# Install
pip install greptimedb-mcp-server

# Run (connects to localhost:4002 by default)
greptimedb-mcp-server --host localhost --database public
```

For Claude Desktop, add this to your config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "greptimedb": {
      "command": "greptimedb-mcp-server",
      "args": ["--host", "localhost", "--database", "public"]
    }
  }
}
```

## Features

### Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL queries with format (csv/json/markdown) and limit options |
| `execute_tql` | Execute TQL (PromQL-compatible) queries for time-series analysis |
| `query_range` | Execute time-window aggregation queries with RANGE/ALIGN syntax |
| `describe_table` | Get table schema including column names, types, and constraints |
| `explain_query` | Analyze SQL or TQL query execution plans |
| `health_check` | Check database connection status and server version |

### Pipeline Management

| Tool | Description |
|------|-------------|
| `list_pipelines` | List all pipelines or get details of a specific pipeline |
| `create_pipeline` | Create a new pipeline with YAML configuration |
| `dryrun_pipeline` | Test a pipeline with sample data without writing to database |
| `delete_pipeline` | Delete a specific version of a pipeline |

### Resources & Prompts

- **Resources**: Browse tables via `greptime://<table>/data` URIs
- **Prompts**: Built-in templates for common tasks — `pipeline_creator`, `log_pipeline`, `metrics_analysis`, `promql_analysis`, `iot_monitoring`, `trace_analysis`, `table_operation`

For LLM integration and prompt usage, see [docs/llm-instructions.md](docs/llm-instructions.md).

## Configuration

### Environment Variables

```bash
GREPTIMEDB_HOST=localhost      # Database host
GREPTIMEDB_PORT=4002           # MySQL protocol port (default: 4002)
GREPTIMEDB_USER=root           # Database user
GREPTIMEDB_PASSWORD=           # Database password
GREPTIMEDB_DATABASE=public     # Database name
GREPTIMEDB_TIMEZONE=UTC        # Session timezone

# Optional
GREPTIMEDB_HTTP_PORT=4000      # HTTP API port for pipeline management
GREPTIMEDB_HTTP_PROTOCOL=http  # HTTP protocol (http/https)
GREPTIMEDB_POOL_SIZE=5         # Connection pool size
GREPTIMEDB_MASK_ENABLED=true   # Enable sensitive data masking
GREPTIMEDB_MASK_PATTERNS=      # Additional patterns (comma-separated)
GREPTIMEDB_AUDIT_ENABLED=true  # Enable audit logging

# Transport (for HTTP server mode)
GREPTIMEDB_TRANSPORT=stdio     # stdio, sse, or streamable-http
GREPTIMEDB_LISTEN_HOST=0.0.0.0 # HTTP server bind host
GREPTIMEDB_LISTEN_PORT=8080    # HTTP server bind port
GREPTIMEDB_ALLOWED_HOSTS=      # DNS rebinding protection (comma-separated)
GREPTIMEDB_ALLOWED_ORIGINS=    # CORS allowed origins (comma-separated)
```

### CLI Arguments

```bash
greptimedb-mcp-server \
  --host localhost \
  --port 4002 \
  --database public \
  --user root \
  --password "" \
  --timezone UTC \
  --pool-size 5 \
  --mask-enabled true \
  --transport stdio
```

### HTTP Server Mode

For containerized or Kubernetes deployments. Requires `mcp>=1.8.0`:

```bash
# Streamable HTTP (recommended for production)
greptimedb-mcp-server --transport streamable-http --listen-port 8080

# SSE mode (legacy)
greptimedb-mcp-server --transport sse --listen-port 3000
```

#### DNS Rebinding Protection

By default, DNS rebinding protection is **disabled** for compatibility with proxies, gateways, and Kubernetes services. To enable it, use `--allowed-hosts`:

```bash
# Enable DNS rebinding protection with allowed hosts
greptimedb-mcp-server --transport streamable-http \
  --allowed-hosts "localhost:*,127.0.0.1:*,my-service.namespace:*"

# With custom allowed origins for CORS
greptimedb-mcp-server --transport streamable-http \
  --allowed-hosts "my-service.namespace:*" \
  --allowed-origins "http://localhost:*,https://my-app.example.com"

# Or via environment variables
GREPTIMEDB_ALLOWED_HOSTS="localhost:*,my-service.namespace:*" \
GREPTIMEDB_ALLOWED_ORIGINS="http://localhost:*" \
  greptimedb-mcp-server --transport streamable-http
```

If you encounter `421 Invalid Host Header` errors, either disable protection (default) or add your host to the allowed list.

## Security

### Read-Only Database User (Recommended)

Create a read-only user in GreptimeDB using [static user provider](https://docs.greptime.com/user-guide/deployments-administration/authentication/static/#permission-modes):

```
mcp_readonly:readonly=your_secure_password
```

### Application-Level Security Gate

All queries go through a security gate that:
- **Blocks**: DROP, DELETE, TRUNCATE, UPDATE, INSERT, ALTER, CREATE, GRANT, REVOKE, EXEC, LOAD, COPY
- **Blocks**: Encoded bypass attempts (hex, UNHEX, CHAR)
- **Allows**: SELECT, SHOW, DESCRIBE, TQL, EXPLAIN, UNION

### Data Masking

Sensitive columns are automatically masked (`******`) based on column name patterns:
- Authentication: `password`, `secret`, `token`, `api_key`, `credential`
- Financial: `credit_card`, `cvv`, `bank_account`
- Personal: `ssn`, `id_card`, `passport`

Configure with `--mask-patterns phone,email` to add custom patterns.

### Audit Logging

All tool invocations are logged:

```
2025-12-10 10:30:45 - greptimedb_mcp_server.audit - INFO - [AUDIT] execute_sql | query="SELECT * FROM cpu LIMIT 10" | success=True | duration_ms=45.2
```

Disable with `--audit-enabled false`.

## Development

```bash
# Clone and setup
git clone https://github.com/GreptimeTeam/greptimedb-mcp-server.git
cd greptimedb-mcp-server
uv venv && source .venv/bin/activate
uv sync

# Run tests
pytest

# Format & lint
uv run black .
uv run flake8 src

# Debug with MCP Inspector
npx @modelcontextprotocol/inspector uv --directory . run -m greptimedb_mcp_server.server
```

## License

MIT License - see [LICENSE.md](LICENSE.md).

## Acknowledgement

Inspired by:
- [ktanaka101/mcp-server-duckdb](https://github.com/ktanaka101/mcp-server-duckdb)
- [designcomputer/mysql_mcp_server](https://github.com/designcomputer/mysql_mcp_server)
- [mikeskarl/mcp-prompt-templates](https://github.com/mikeskarl/mcp-prompt-templates)
