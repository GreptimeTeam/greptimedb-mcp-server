from greptimedb_mcp_server.config import Config
from greptimedb_mcp_server.utils import security_gate, templates_loader

import asyncio
import logging
from logging import Logger
from mysql.connector import connect, Error
from mcp.server import Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    Prompt,
    GetPromptResult,
    PromptMessage,
)
from pydantic import AnyUrl

# Resource URI prefix
RES_PREFIX = "greptime://"
# Resource query results limit
RESULTS_LIMIT = 100

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# The GreptimeDB MCP Server
class DatabaseServer:
    def __init__(self, logger: Logger, config: Config):
        """Initialize the GreptimeDB MCP server"""
        self.app = Server("greptimedb_mcp_server")
        self.logger = logger
        self.db_config = {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
            "database": config.database,
        }
        self.templates = templates_loader()

        self.logger.info(f"GreptimeDB Config: {self.db_config}")

        # Register callbacks
        self.app.list_resources()(self.list_resources)
        self.app.read_resource()(self.read_resource)
        self.app.list_prompts()(self.list_prompts)
        self.app.get_prompt()(self.get_prompt)
        self.app.list_tools()(self.list_tools)
        self.app.call_tool()(self.call_tool)

    async def list_resources(self) -> list[Resource]:
        """List GreptimeDB tables as resources."""
        logger = self.logger
        config = self.db_config

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
                                uri=f"{RES_PREFIX}{table[0]}/data",
                                name=f"Table: {table[0]}",
                                mimeType="text/plain",
                                description=f"Data in table: {table[0]}",
                            )
                        )
                    return resources
        except Error as e:
            logger.error(f"Failed to list resources: {str(e)}")
            return []

    async def read_resource(self, uri: AnyUrl) -> str:
        """Read table contents."""
        logger = self.logger
        config = self.db_config

        uri_str = str(uri)
        logger.info(f"Reading resource: {uri_str}")

        if not uri_str.startswith(RES_PREFIX):
            raise ValueError(f"Invalid URI scheme: {uri_str}")

        parts = uri_str[len(RES_PREFIX) :].split("/")
        table = parts[0]

        try:
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {table} LIMIT {RESULTS_LIMIT}")
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
            return "\n".join([",".join(columns)] + result)

        except Error as e:
            logger.error(f"Database error reading resource {uri}: {str(e)}")
            raise RuntimeError(f"Database error: {str(e)}")

    async def list_prompts(self) -> list[Prompt]:
        """List available GreptimeDB prompts."""
        logger = self.logger

        logger.info("Listing prompts...")
        prompts = []
        for name, template in self.templates.items():
            logger.info(f"Found prompt: {name}")
            prompts.append(
                Prompt(
                    name=name,
                    description=template["config"]["description"],
                    arguments=template["config"]["arguments"],
                )
            )
        return prompts

    async def get_prompt(
        self, name: str, arguments: dict[str, str] | None
    ) -> GetPromptResult:
        """Handle the get_prompt request."""
        logger = self.logger

        logger.info(f"Get prompt: {name}")
        if name not in self.templates:
            logger.error(f"Unknown template: {name}")
            raise ValueError(f"Unknown template: {name}")

        template = self.templates[name]
        formatted_template = template["template"]

        # Replace placeholders with arguments
        if arguments:
            for key, value in arguments.items():
                formatted_template = formatted_template.replace(
                    f"{{{{ {key} }}}}", value
                )

        return GetPromptResult(
            description=template["config"]["description"],
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(type="text", text=formatted_template),
                )
            ],
        )

    async def list_tools(self) -> list[Tool]:
        """List available GreptimeDB tools."""
        logger = self.logger

        logger.info("Listing tools...")
        return [
            Tool(
                name="execute_sql",
                description="Execute SQL query against GreptimeDB. Please use MySQL dialect when generating SQL queries.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The SQL query to execute (using MySQL dialect)",
                        }
                    },
                    "required": ["query"],
                },
            )
        ]

    async def call_tool(self, name: str, arguments: dict) -> list[TextContent]:
        """Execute SQL commands."""
        logger = self.logger
        config = self.db_config

        logger.info(f"Calling tool: {name} with arguments: {arguments}")

        if name != "execute_sql":
            raise ValueError(f"Unknown tool: {name}")

        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")

        # Check if query is dangerous
        is_dangerous, reason = security_gate(query=query)
        if is_dangerous:
            return [
                TextContent(
                    type="text",
                    text="Error: Contain dangerous operations, reason:" + reason,
                )
            ]

        try:
            with connect(**config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)

                    stmt = query.strip().upper()
                    # Special handling for SHOW TABLES
                    if stmt.startswith("SHOW TABLES"):
                        tables = cursor.fetchall()
                        result = ["Tables_in_" + config["database"]]  # Header
                        result.extend([table[0] for table in tables])
                        return [TextContent(type="text", text="\n".join(result))]
                    # Regular SELECT queries
                    elif stmt.startswith("SELECT") or stmt.startswith("DESCRIBE"):
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        result = [",".join(map(str, row)) for row in rows]
                        return [
                            TextContent(
                                type="text",
                                text="\n".join([",".join(columns)] + result),
                            )
                        ]

                    # Non-SELECT queries
                    else:
                        conn.commit()
                        return [
                            TextContent(
                                type="text",
                                text=f"Query executed successfully. Rows affected: {cursor.rowcount}",
                            )
                        ]

        except Error as e:
            logger.error(f"Error executing SQL '{query}': {e}")
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

    async def run(self):
        """Run the MCP server."""
        logger = self.logger
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            try:
                await self.app.run(
                    read_stream, write_stream, self.app.create_initialization_options()
                )
            except Exception as e:
                logger.error(f"Server error: {str(e)}", exc_info=True)
                raise


async def main(config: Config):
    """Main entry point to run the MCP server."""
    logger = logging.getLogger("greptimedb_mcp_server")
    db_server = DatabaseServer(logger, config)

    logger.info("Starting GreptimeDB MCP server...")

    await db_server.run()


if __name__ == "__main__":
    asyncio.run(main(Config.from_env_arguments()))
