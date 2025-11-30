# greptimedb-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/greptimedb-mcp-server)](https://pypi.org/project/greptimedb-mcp-server/)
![build workflow](https://github.com/GreptimeTeam/greptimedb-mcp-server/actions/workflows/python-app.yml/badge.svg)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

A Model Context Protocol (MCP) server implementation for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb).

This server provides AI assistants with a secure and structured way to explore and analyze databases. It enables them to list tables, read data, and execute SQL queries through a controlled interface, ensuring responsible database access.

# Project Status
This is an experimental project that is still under development. Data security and privacy issues have not been specifically addressed, so please use it with caution.

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

## Prompts
- **list_prompts** - List available prompt templates
- **get_prompt** - Get a prompt template by name with argument substitution

## Security
All queries pass through a security gate that:
- Blocks DDL/DML operations: DROP, DELETE, TRUNCATE, UPDATE, INSERT, ALTER, CREATE, GRANT, REVOKE
- Blocks dynamic SQL execution: EXEC, EXECUTE, CALL
- Blocks data modification: REPLACE INTO
- Blocks file system access: LOAD, COPY, OUTFILE, LOAD_FILE, INTO DUMPFILE
- Blocks encoded content bypass attempts: hex encoding (0x...), UNHEX(), CHAR()
- Prevents multiple statement execution with dangerous operations
- Allows read-only operations: SELECT, SHOW, DESCRIBE, TQL, EXPLAIN, UNION, INFORMATION_SCHEMA

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

## Performance
- Connection pooling for efficient database access
- Async operations for non-blocking execution

# Installation

```
pip install greptimedb-mcp-server
```


# Configuration

Set the following environment variables:

```bash
GREPTIMEDB_HOST=localhost    # Database host
GREPTIMEDB_PORT=4002         # Optional: Database MySQL port (defaults to 4002 if not specified)
GREPTIMEDB_USER=root
GREPTIMEDB_PASSWORD=
GREPTIMEDB_DATABASE=public
GREPTIMEDB_TIMEZONE=UTC
GREPTIMEDB_POOL_SIZE=5       # Optional: Connection pool size (defaults to 5)
GREPTIMEDB_MASK_ENABLED=true # Optional: Enable data masking (defaults to true)
GREPTIMEDB_MASK_PATTERNS=    # Optional: Additional sensitive column patterns (comma-separated)
```

Or via command-line args:

* `--host` the database host, `localhost` by default,
* `--port` the database port, must be MySQL protocol port,  `4002` by default,
* `--user` the database username, empty by default,
* `--password` the database password, empty by default,
* `--database` the database name, `public` by default,
* `--timezone` the session time zone, empty by default (using server default time zone),
* `--pool-size` the connection pool size, `5` by default,
* `--mask-enabled` enable data masking for sensitive columns, `true` by default,
* `--mask-patterns` additional sensitive column patterns (comma-separated), empty by default.

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

## Claude Desktop Integration

Configure the MCP server in Claude Desktop's configuration file:

#### MacOS

Location: `~/Library/Application Support/Claude/claude_desktop_config.json`

#### Windows

Location: `%APPDATA%/Claude/claude_desktop_config.json`


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
- Python with `uv` package manager
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
