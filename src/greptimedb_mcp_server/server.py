from greptimedb_mcp_server.config import Config
from greptimedb_mcp_server.utils import security_gate, templates_loader

import datetime
import asyncio
import re
import logging
import json
import time
from logging import Logger
from mysql.connector import connect, Error
from mysql.connector.pooling import MySQLConnectionPool
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
# Maximum query limit
MAX_QUERY_LIMIT = 10000
# Valid output formats
VALID_FORMATS = {"csv", "json", "markdown"}
# Table name validation pattern
TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def format_value(value):
    """Quote string and datetime values, leave others as-is"""
    if isinstance(value, (str, datetime.datetime, datetime.date, datetime.time)):
        return f'"{value}"'
    return str(value)


def format_results(columns: list, rows: list, fmt: str = "csv") -> str:
    """Format query results in specified format."""
    if fmt == "json":
        result = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
                    val = str(val)
                row_dict[col] = val
            result.append(row_dict)
        return json.dumps(result, ensure_ascii=False, indent=2)
    elif fmt == "markdown":
        if not rows:
            return (
                "| "
                + " | ".join(columns)
                + " |\n"
                + "| "
                + " | ".join(["---"] * len(columns))
                + " |"
            )
        lines = []
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
        for row in rows:
            formatted = [str(v) if v is not None else "" for v in row]
            lines.append("| " + " | ".join(formatted) + " |")
        return "\n".join(lines)
    else:  # csv
        result = [",".join(format_value(val) for val in row) for row in rows]
        return "\n".join([",".join(columns)] + result)


def validate_table_name(table: str) -> str:
    """Validate table name format."""
    if not table:
        raise ValueError("Table name is required")
    if not TABLE_NAME_PATTERN.match(table):
        raise ValueError("Invalid table name")
    return table


def validate_tql_param(value: str, name: str) -> str:
    """Validate TQL parameter doesn't contain injection characters."""
    if not value:
        raise ValueError(f"{name} is required")
    if "'" in value or ";" in value or "--" in value:
        raise ValueError(f"Invalid characters in {name}")
    return value


def _security_error(reason: str) -> list[TextContent]:
    """Create a security error response."""
    return [
        TextContent(type="text", text=f"Error: Dangerous operation blocked: {reason}")
    ]


def validate_query_component(value: str, name: str) -> str:
    """Validate query component via security gate."""
    if not value:
        return value
    is_dangerous, reason = security_gate(value)
    if is_dangerous:
        raise ValueError(f"Dangerous pattern in {name}: {reason}")
    return value


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
            "time_zone": config.time_zone,
        }
        self.templates = templates_loader()

        # Initialize connection pool
        self._pool = None
        self._pool_config = {
            "pool_name": "greptimedb_pool",
            "pool_size": 5,
            "pool_reset_session": True,
            **self.db_config,
        }

        self.logger.info(f"GreptimeDB Config: {self.db_config}")

        # Register callbacks
        self.app.list_resources()(self.list_resources)
        self.app.read_resource()(self.read_resource)
        self.app.list_prompts()(self.list_prompts)
        self.app.get_prompt()(self.get_prompt)
        self.app.list_tools()(self.list_tools)
        self.app.call_tool()(self.call_tool)

    def _build_query_response(
        self, query_info: str, columns: list, rows: list, elapsed_ms: float
    ) -> list[TextContent]:
        """Build a JSON response for query results."""
        result = format_results(columns, rows, "json")
        meta = {
            "query": query_info,
            "data": json.loads(result),
            "row_count": len(rows),
            "execution_time_ms": round(elapsed_ms, 2),
        }
        return [
            TextContent(
                type="text", text=json.dumps(meta, indent=2, ensure_ascii=False)
            )
        ]

    def _get_connection(self):
        """Get a connection from the pool, creating pool if needed."""
        if self._pool is None:
            try:
                self._pool = MySQLConnectionPool(**self._pool_config)
                self.logger.info("Connection pool created")
            except Error as e:
                self.logger.warning(
                    f"Failed to create pool, using direct connection: {e}"
                )
                return connect(**self.db_config)
        try:
            return self._pool.get_connection()
        except Error as e:
            self.logger.warning(f"Failed to get pool connection, using direct: {e}")
            return connect(**self.db_config)

    async def list_resources(self) -> list[Resource]:
        """List GreptimeDB tables as resources."""

        def _sync_list_tables():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    return cursor.fetchall()

        try:
            tables = await asyncio.to_thread(_sync_list_tables)
            self.logger.info(f"Found tables: {tables}")

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
            self.logger.error(f"Failed to list resources: {str(e)}")
            return []

    async def read_resource(self, uri: AnyUrl) -> str:
        """Read table contents."""
        uri_str = str(uri)
        self.logger.info(f"Reading resource: {uri_str}")

        if not uri_str.startswith(RES_PREFIX):
            raise ValueError(f"Invalid URI scheme: {uri_str}")

        parts = uri_str[len(RES_PREFIX) :].split("/")
        table = validate_table_name(parts[0])

        def _sync_read_table():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {table} LIMIT %s", (RESULTS_LIMIT,))
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [
                        ",".join(format_value(val) for val in row) for row in rows
                    ]
                    return "\n".join([",".join(columns)] + result)

        try:
            return await asyncio.to_thread(_sync_read_table)
        except Error as e:
            self.logger.error(f"Database error reading resource {uri}: {str(e)}")
            raise RuntimeError(f"Database error: {str(e)}")

    async def list_prompts(self) -> list[Prompt]:
        """List available GreptimeDB prompts."""
        self.logger.info("Listing prompts...")
        prompts = []
        for name, template in self.templates.items():
            self.logger.info(f"Found prompt: {name}")
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
        self.logger.info(f"Get prompt: {name}")
        if name not in self.templates:
            self.logger.error(f"Unknown template: {name}")
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
        self.logger.info("Listing tools...")
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
                        },
                        "format": {
                            "type": "string",
                            "description": "Output format: csv, json, or markdown (default: csv)",
                            "enum": ["csv", "json", "markdown"],
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of rows to return (default: 1000)",
                        },
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="describe_table",
                description="Get table schema information including column names, types, and constraints.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "The table name to describe",
                        },
                    },
                    "required": ["table"],
                },
            ),
            Tool(
                name="health_check",
                description="Check GreptimeDB connection status and server version.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="execute_tql",
                description="Execute TQL (PromQL-compatible) query for time-series analysis. "
                "TQL extends PromQL with SQL integration for powerful time-series queries.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "PromQL expression, e.g., rate(http_requests_total[5m])",
                        },
                        "start": {
                            "type": "string",
                            "description": "Start time (RFC3339, Unix timestamp, or relative like 'now-1h')",
                        },
                        "end": {
                            "type": "string",
                            "description": "End time (RFC3339, Unix timestamp, or relative like 'now')",
                        },
                        "step": {
                            "type": "string",
                            "description": "Query resolution step, e.g., '1m', '5m', '1h'",
                        },
                        "lookback": {
                            "type": "string",
                            "description": "(Optional) Lookback delta for range queries",
                        },
                    },
                    "required": ["query", "start", "end", "step"],
                },
            ),
            Tool(
                name="query_range",
                description="Execute time-window aggregation query using GreptimeDB's RANGE query syntax. "
                "Ideal for downsampling and time-series aggregation.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Table name to query",
                        },
                        "select": {
                            "type": "string",
                            "description": "Columns and aggregations, e.g., 'ts, host, avg(cpu) RANGE \\'5m\\''",
                        },
                        "align": {
                            "type": "string",
                            "description": "Alignment interval, e.g., '1m', '5m'",
                        },
                        "by": {
                            "type": "string",
                            "description": "(Optional) Group by columns, e.g., 'host'",
                        },
                        "where": {
                            "type": "string",
                            "description": "(Optional) WHERE clause conditions",
                        },
                        "fill": {
                            "type": "string",
                            "description": "(Optional) Fill strategy: NULL, PREV, LINEAR, or a value",
                        },
                        "order_by": {
                            "type": "string",
                            "description": "(Optional) ORDER BY clause, default: 'ts DESC'",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "(Optional) Maximum rows to return, default: 1000",
                        },
                    },
                    "required": ["table", "select", "align"],
                },
            ),
            Tool(
                name="explain_query",
                description="Analyze SQL or TQL query execution plan. "
                "Use 'analyze: true' to execute and show actual metrics.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL or TQL query to analyze",
                        },
                        "analyze": {
                            "type": "boolean",
                            "description": "(Optional) Execute and show actual metrics, default: false",
                        },
                    },
                    "required": ["query"],
                },
            ),
        ]

    async def call_tool(self, name: str, arguments: dict) -> list[TextContent]:
        """Execute database tools."""
        self.logger.info(f"Calling tool: {name} with arguments: {arguments}")

        if name == "health_check":
            return await self._health_check()
        elif name == "describe_table":
            return await self._describe_table(arguments)
        elif name == "execute_sql":
            return await self._execute_sql(arguments)
        elif name == "execute_tql":
            return await self._execute_tql(arguments)
        elif name == "query_range":
            return await self._query_range(arguments)
        elif name == "explain_query":
            return await self._explain_query(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

    async def _health_check(self) -> list[TextContent]:
        """Check database connection and server status."""
        start_time = time.time()

        def _sync_health_check():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.execute("SELECT version()")
                    version_row = cursor.fetchone()
                    return version_row[0] if version_row else "unknown"

        try:
            version = await asyncio.to_thread(_sync_health_check)
            elapsed_ms = (time.time() - start_time) * 1000
            result = {
                "status": "healthy",
                "host": self.db_config["host"],
                "port": self.db_config["port"],
                "database": self.db_config["database"],
                "version": version,
                "response_time_ms": round(elapsed_ms, 2),
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

        except Error as e:
            self.logger.error(f"Health check failed: {e}")
            result = {
                "status": "unhealthy",
                "error": str(e),
                "host": self.db_config["host"],
                "port": self.db_config["port"],
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]

    async def _describe_table(self, arguments: dict) -> list[TextContent]:
        """Get table schema information."""
        table = validate_table_name(arguments.get("table"))

        def _sync_describe():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE {table}")
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return format_results(columns, rows, "markdown")

        try:
            result = await asyncio.to_thread(_sync_describe)
            return [TextContent(type="text", text=result)]
        except Error as e:
            self.logger.error(f"Error describing table '{table}': {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _execute_sql(self, arguments: dict) -> list[TextContent]:
        """Execute SQL query with format and limit options."""
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")

        fmt = arguments.get("format", "csv")
        if fmt not in VALID_FORMATS:
            raise ValueError(f"Invalid format: {fmt}. Must be one of: {VALID_FORMATS}")

        limit = arguments.get("limit", 1000)
        limit = min(max(1, limit), MAX_QUERY_LIMIT)

        is_dangerous, reason = security_gate(query=query)
        if is_dangerous:
            return _security_error(reason)

        start_time = time.time()

        def _sync_execute():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    stmt = query.strip().upper()

                    if stmt.startswith("SHOW DATABASES"):
                        dbs = cursor.fetchall()
                        result = ["Databases"]
                        result.extend([db[0] for db in dbs])
                        return {"type": "simple", "text": "\n".join(result)}

                    if stmt.startswith("SHOW TABLES"):
                        tables = cursor.fetchall()
                        result = ["Tables_in_" + self.db_config["database"]]
                        result.extend([t[0] for t in tables])
                        return {"type": "simple", "text": "\n".join(result)}

                    if any(
                        stmt.startswith(cmd)
                        for cmd in ["SELECT", "SHOW", "DESC", "TQL", "EXPLAIN", "WITH"]
                    ):
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchmany(limit)
                        has_more = cursor.fetchone() is not None
                        return {
                            "type": "query",
                            "columns": columns,
                            "rows": rows,
                            "has_more": has_more,
                        }

                    conn.commit()
                    return {"type": "modify", "rowcount": cursor.rowcount}

        try:
            result = await asyncio.to_thread(_sync_execute)
            elapsed_ms = (time.time() - start_time) * 1000

            if result["type"] == "simple":
                return [TextContent(type="text", text=result["text"])]

            if result["type"] == "modify":
                return [
                    TextContent(
                        type="text",
                        text=f"Query executed successfully. Rows affected: {result['rowcount']}",
                    )
                ]

            # Query result
            columns = result["columns"]
            rows = result["rows"]
            has_more = result["has_more"]
            formatted = format_results(columns, rows, fmt)

            if fmt == "json":
                meta = {
                    "data": json.loads(formatted),
                    "row_count": len(rows),
                    "truncated": has_more,
                    "execution_time_ms": round(elapsed_ms, 2),
                }
                return [
                    TextContent(
                        type="text", text=json.dumps(meta, indent=2, ensure_ascii=False)
                    )
                ]

            return [TextContent(type="text", text=formatted)]

        except Error as e:
            self.logger.error(f"Error executing SQL '{query}': {e}")
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

    async def _execute_tql(self, arguments: dict) -> list[TextContent]:
        """Execute TQL (PromQL) query."""
        query = arguments.get("query")
        start = arguments.get("start")
        end = arguments.get("end")
        step = arguments.get("step")
        lookback = arguments.get("lookback")

        if not all([query, start, end, step]):
            raise ValueError("query, start, end, and step are required")

        validate_tql_param(start, "start")
        validate_tql_param(end, "end")
        validate_tql_param(step, "step")
        if lookback:
            validate_tql_param(lookback, "lookback")

        is_dangerous, reason = security_gate(query)
        if is_dangerous:
            return _security_error(reason)

        if lookback:
            tql = f"TQL EVAL ('{start}', '{end}', '{step}', '{lookback}') {query}"
        else:
            tql = f"TQL EVAL ('{start}', '{end}', '{step}') {query}"

        start_time = time.time()

        def _sync_tql():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(tql)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return columns, rows

        try:
            columns, rows = await asyncio.to_thread(_sync_tql)
            elapsed_ms = (time.time() - start_time) * 1000
            result = format_results(columns, rows, "json")

            meta = {
                "tql": tql,
                "data": json.loads(result),
                "row_count": len(rows),
                "execution_time_ms": round(elapsed_ms, 2),
            }
            return [
                TextContent(
                    type="text", text=json.dumps(meta, indent=2, ensure_ascii=False)
                )
            ]

        except Error as e:
            self.logger.error(f"Error executing TQL '{tql}': {e}")
            return [TextContent(type="text", text=f"Error executing TQL: {str(e)}")]

    async def _query_range(self, arguments: dict) -> list[TextContent]:
        """Execute GreptimeDB range query."""
        table = arguments.get("table")
        select = arguments.get("select")
        align = arguments.get("align")
        by = arguments.get("by")
        where = arguments.get("where")
        order_by = arguments.get("order_by", "ts DESC")
        limit = arguments.get("limit", 1000)

        if not all([table, select, align]):
            raise ValueError("table, select, and align are required")

        validate_table_name(table)
        validate_query_component(select, "select")
        validate_query_component(where, "where")
        validate_query_component(by, "by")
        validate_query_component(order_by, "order_by")
        limit = min(max(1, limit), MAX_QUERY_LIMIT)

        query_parts = [f"SELECT {select}", f"FROM {table}"]

        if where:
            query_parts.append(f"WHERE {where}")

        query_parts.append(f"ALIGN '{align}'")

        if by:
            query_parts.append(f"BY ({by})")

        query_parts.append(f"ORDER BY {order_by}")
        query_parts.append(f"LIMIT {limit}")

        query = " ".join(query_parts)

        is_dangerous, reason = security_gate(query=query)
        if is_dangerous:
            return _security_error(reason)

        start_time = time.time()

        def _sync_range():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchmany(limit)
                    return columns, rows

        try:
            columns, rows = await asyncio.to_thread(_sync_range)
            elapsed_ms = (time.time() - start_time) * 1000
            result = format_results(columns, rows, "json")

            meta = {
                "query": query,
                "data": json.loads(result),
                "row_count": len(rows),
                "execution_time_ms": round(elapsed_ms, 2),
            }
            return [
                TextContent(
                    type="text", text=json.dumps(meta, indent=2, ensure_ascii=False)
                )
            ]

        except Error as e:
            self.logger.error(f"Error executing range query '{query}': {e}")
            return [
                TextContent(type="text", text=f"Error executing range query: {str(e)}")
            ]

    async def _explain_query(self, arguments: dict) -> list[TextContent]:
        """Explain query execution plan."""
        query = arguments.get("query")
        analyze = arguments.get("analyze", False)

        if not query:
            raise ValueError("query is required")

        is_dangerous, reason = security_gate(query)
        if is_dangerous:
            return _security_error(reason)

        if query.strip().upper().startswith("TQL"):
            if analyze:
                explain_query = query.replace("TQL EVAL", "TQL ANALYZE", 1)
                explain_query = explain_query.replace("TQL EVALUATE", "TQL ANALYZE", 1)
            else:
                explain_query = query.replace("TQL EVAL", "TQL EXPLAIN", 1)
                explain_query = explain_query.replace("TQL EVALUATE", "TQL EXPLAIN", 1)
        else:
            if analyze:
                explain_query = f"EXPLAIN ANALYZE {query}"
            else:
                explain_query = f"EXPLAIN {query}"

        def _sync_explain():
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(explain_query)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return format_results(columns, rows, "markdown")

        try:
            result = await asyncio.to_thread(_sync_explain)
            return [TextContent(type="text", text=result)]
        except Error as e:
            self.logger.error(f"Error explaining query '{query}': {e}")
            return [TextContent(type="text", text=f"Error explaining query: {str(e)}")]

    async def run(self):
        """Run the MCP server."""
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            try:
                await self.app.run(
                    read_stream, write_stream, self.app.create_initialization_options()
                )
            except Exception as e:
                self.logger.error(f"Server error: {str(e)}", exc_info=True)
                raise


async def main(config: Config):
    """Main entry point to run the MCP server."""
    logger = logging.getLogger("greptimedb_mcp_server")
    db_server = DatabaseServer(logger, config)

    logger.info("Starting GreptimeDB MCP server...")

    await db_server.run()


if __name__ == "__main__":
    asyncio.run(main(Config.from_env_arguments()))
