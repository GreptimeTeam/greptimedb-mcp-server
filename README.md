# greptimedb-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/greptimedb-mcp-server)](https://pypi.org/project/greptimedb-mcp-server/)
![build workflow](https://github.com/GreptimeTeam/greptimedb-mcp-server/actions/workflows/python-app.yml/badge.svg)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

A Model Context Protocol (MCP) server implementation for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb).

This server provides AI assistants with a secure and structured way to explore and analyze databases. It enables them to list tables, read data, and execute SQL queries through a controlled interface, ensuring responsible database access.

# Project Status
This is an experimental project that is still under development. Data security and privacy issues have not been specifically addressed, so please use it with caution.

# Capabilities

* `list_resources` to list tables
* `read_resource` to read table data
* `list_tools` to list tools
* `call_tool` to execute an SQL
* `list_prompts` to list prompts
* `get_prompt` to get the prompt by name

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
```

Or via command-line args:

* `--host` the database host, `localhost` by default,
* `--port` the database port, must be MySQL protocol port,  `4002` by default,
* `--user` the database username, empty by default,
* `--password` the database password, empty by default,
* `--database` the database name, `public` by default.
* `--timezone` the session time zone, empty by default(using server default time zone).

# Usage

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
        "GREPTIMEDB_TIMEZONE": ""
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
