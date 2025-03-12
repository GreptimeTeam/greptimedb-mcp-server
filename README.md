# greptimedb-mcp-server
A Model Context Protocol (MCP) server implementation for GreptimeDB.

This server provides AI assistants with a secure and structured way to explore and analyze databases. It enables them to list tables, read data, and execute SQL queries through a controlled interface, ensuring responsible database access.

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
        "GREPTIMEDB_PORT": "3306",
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
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest
```

# Acknowledgement
This library's implementation was inspired by the following two repositories and incorporates their code, for which we express our gratitude：

* [ktanaka101/mcp-server-duckdb](https://github.com/ktanaka101/mcp-server-duckdb)
* [designcomputer/mysql_mcp_server)](https://github.com/designcomputer/mysql_mcp_server)

Thanks!