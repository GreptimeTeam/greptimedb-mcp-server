from greptimedb_mcp_server.config import Config

import asyncio
import logging
from mysql.connector import connect, Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main(config: Config):
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server


    logger = logging.getLogger("greptimedb_mcp_server")

    # Initialize server
    app = Server("greptimedb_mcp_server")

    config = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "database": config.database
    }

    logger.info(f"GreptimeDB Config: {config}")

    @app.list_resources()
    async def list_resources() -> list[Resource]:
        """List GreptimeDB tables as resources."""
        try:
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    logger.info(f"Found tables: {tables}")

                    resources = []
                    for table in tables:
                        resources.append(
                            Resource(
                                uri=f"greptime://{table[0]}/data",
                                name=f"Table: {table[0]}",
                                mimeType="text/plain",
                                description=f"Data in table: {table[0]}"
                            )
                        )
                        return resources
        except Error as e:
            logger.error(f"Failed to list resources: {str(e)}")
            return []

    @app.read_resource()
    async def read_resource(uri: AnyUrl) -> str:
        """Read table contents."""
        uri_str = str(uri)
        logger.info(f"Reading resource: {uri_str}")

        if not uri_str.startswith("greptime://"):
            raise ValueError(f"Invalid URI scheme: {uri_str}")

        parts = uri_str[8:].split('/')
        table = parts[0]

        try:
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {table} LIMIT 100")
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return "\n".join([",".join(columns)] + result)

        except Error as e:
            logger.error(f"Database error reading resource {uri}: {str(e)}")
            raise RuntimeError(f"Database error: {str(e)}")

    @app.list_tools()
    async def list_tools() -> list[Tool]:
        """List available GreptimeDB tools."""
        logger.info("Listing tools...")
        return [
            Tool(
                name="execute_sql",
                description="Execute an SQL query on the GreptimeDB server",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The SQL query to execute"
                        }
                    },
                    "required": ["query"]
                }
            )
        ]

    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        """Execute SQL commands."""
        logger.info(f"Calling tool: {name} with arguments: {arguments}")

        if name != "execute_sql":
            raise ValueError(f"Unknown tool: {name}")

        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")

        try:
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)

                    # Special handling for SHOW TABLES
                    if query.strip().upper().startswith("SHOW TABLES"):
                        tables = cursor.fetchall()
                        result = ["Tables_in_" + config["database"]]  # Header
                        result.extend([table[0] for table in tables])
                        return [TextContent(type="text", text="\n".join(result))]

                    # Regular SELECT queries
                    elif query.strip().upper().startswith("SELECT"):
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        result = [",".join(map(str, row)) for row in rows]
                        return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]

                    # Non-SELECT queries
                    else:
                        conn.commit()
                        return eturn [TextContent(type="text", text=f"Query executed successfully. Rows affected: {cursor.rowcount}")]

        except Error as e:
            logger.error(f"Error executing SQL '{query}': {e}")
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]


    logger.info("Starting GreptimeDB MCP server...")
    logger.info(f"Database config: {config['host']}/{config['database']} as {config['user']}")


    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    asyncio.run(main(Config.from_env_arguments()))
