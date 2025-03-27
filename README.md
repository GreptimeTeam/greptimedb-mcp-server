# greptimedb-mcp-server
A Model Context Protocol (MCP) server implementation for [GreptimeDB](https://github.com/GreptimeTeam/greptimedb).

This server provides AI assistants with a secure and structured way to explore and analyze databases. It enables them to list tables, read data, and execute SQL queries through a controlled interface, ensuring responsible database access.

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
GREPTIMEDB_PORT=4002         # Optional: Database port (defaults to 4002 if not specified)
GREPTIMEDB_USER=root
GREPTIMEDB_PASSWORD=
GREPTIMEDB_DATABASE=public
```

Or via command-line args:

* `--host` the database host
* `--port` the database port
* `--user` the database username
* `--password` the database password
* `--database` the database name

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
        "GREPTIMEDB_DATABASE": "public"
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
