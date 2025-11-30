"""GreptimeDB MCP Server using FastMCP API."""

from greptimedb_mcp_server.config import Config
from greptimedb_mcp_server.utils import security_gate, templates_loader

import asyncio
import csv
import datetime
import io
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Annotated

from mcp.server.fastmcp import FastMCP, Context
from mysql.connector import connect, Error
from mysql.connector.pooling import MySQLConnectionPool

# Constants
RES_PREFIX = "greptime://"
RESULTS_LIMIT = 100
MAX_QUERY_LIMIT = 10000
VALID_FORMATS = {"csv", "json", "markdown"}
TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("greptimedb_mcp_server")


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

        def escape_md(val):
            if val is None:
                return ""
            s = str(val)
            s = s.replace("\\", "\\\\")
            s = s.replace("|", "\\|")
            s = s.replace("\n", " ")
            s = s.replace("\r", "")
            return s

        escaped_cols = [c.replace("|", "\\|") for c in columns]
        if not rows:
            return (
                "| "
                + " | ".join(escaped_cols)
                + " |\n"
                + "| "
                + " | ".join(["---"] * len(columns))
                + " |"
            )
        lines = []
        lines.append("| " + " | ".join(escaped_cols) + " |")
        lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
        for row in rows:
            formatted = [escape_md(v) for v in row]
            lines.append("| " + " | ".join(formatted) + " |")
        return "\n".join(lines)
    else:  # csv
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            # Convert datetime objects to strings for CSV
            formatted_row = [
                (
                    str(v)
                    if isinstance(v, (datetime.datetime, datetime.date, datetime.time))
                    else v
                )
                for v in row
            ]
            writer.writerow(formatted_row)
        return output.getvalue().rstrip("\r\n")


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


def validate_query_component(value: str, name: str) -> str:
    """Validate query component via security gate."""
    if not value:
        return value
    is_dangerous, reason = security_gate(value)
    if is_dangerous:
        raise ValueError(f"Dangerous pattern in {name}: {reason}")
    return value


# Prometheus duration pattern: number + unit (ms, s, m, h, d, w, y)
DURATION_PATTERN = re.compile(r"^(\d+)(ms|s|m|h|d|w|y)$")


def validate_duration(value: str, name: str) -> str:
    """Validate duration parameter follows Prometheus duration syntax."""
    if not value:
        raise ValueError(f"{name} is required")
    if not DURATION_PATTERN.match(value):
        raise ValueError(
            f"Invalid {name}: must be a duration like '1m', '5m', '1h', '30s'"
        )
    return value


# Valid FILL values: NULL, PREV, LINEAR, or numeric
FILL_PATTERN = re.compile(r"^(NULL|PREV|LINEAR|(-?\d+(\.\d+)?))$", re.IGNORECASE)


def validate_fill(value: str) -> str:
    """Validate FILL parameter."""
    if not value:
        return value
    if not FILL_PATTERN.match(value):
        raise ValueError("Invalid fill: must be NULL, PREV, LINEAR, or a number")
    return value


@dataclass
class AppState:
    """Application state shared across tools."""

    db_config: dict
    pool_config: dict
    templates: dict
    pool: MySQLConnectionPool | None = field(default=None)

    def get_connection(self):
        """Get a connection from the pool, creating pool if needed."""
        if self.pool is None:
            try:
                self.pool = MySQLConnectionPool(**self.pool_config)
                logger.info("Connection pool created")
            except Error as e:
                logger.warning(f"Failed to create pool, using direct connection: {e}")
                return connect(**self.db_config)
        try:
            return self.pool.get_connection()
        except Error as e:
            logger.warning(f"Failed to get pool connection, using direct: {e}")
            return connect(**self.db_config)


# Global state (initialized in lifespan)
_state: AppState | None = None


def get_state() -> AppState:
    """Get the application state."""
    if _state is None:
        raise RuntimeError("Application state not initialized")
    return _state


@asynccontextmanager
async def lifespan(mcp: FastMCP):
    """Initialize application state on startup."""
    global _state

    config = Config.from_env_arguments()
    db_config = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "database": config.database,
        "time_zone": config.time_zone,
    }
    pool_config = {
        "pool_name": "greptimedb_pool",
        "pool_size": 5,
        "pool_reset_session": True,
        **db_config,
    }

    _state = AppState(
        db_config=db_config,
        pool_config=pool_config,
        templates=templates_loader(),
    )

    logger.info(f"GreptimeDB Config: {db_config}")
    logger.info("Starting GreptimeDB MCP server...")

    yield _state

    logger.info("Shutting down GreptimeDB MCP server...")


mcp = FastMCP(
    "greptimedb_mcp_server",
    instructions="GreptimeDB MCP Server - provides secure read-only access to GreptimeDB",
    lifespan=lifespan,
)


@mcp.tool()
async def execute_sql(
    query: Annotated[str, "The SQL query to execute (using MySQL dialect)"],
    format: Annotated[
        str, "Output format: csv, json, or markdown (default: csv)"
    ] = "csv",
    limit: Annotated[int, "Maximum number of rows to return (default: 1000)"] = 1000,
) -> str:
    """Execute SQL query against GreptimeDB. Please use MySQL dialect."""
    state = get_state()

    if not query:
        raise ValueError("Query is required")

    if format not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format}. Must be one of: {VALID_FORMATS}")

    limit = min(max(1, limit), MAX_QUERY_LIMIT)

    is_dangerous, reason = security_gate(query=query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    start_time = time.time()

    def _sync_execute():
        with state.get_connection() as conn:
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
                    result = ["Tables_in_" + state.db_config["database"]]
                    result.extend([t[0] for t in tables])
                    return {"type": "simple", "text": "\n".join(result)}

                if any(
                    stmt.startswith(cmd)
                    for cmd in ["SELECT", "SHOW", "DESC", "TQL", "EXPLAIN", "WITH"]
                ):
                    if cursor.description is None:
                        return {"type": "error", "message": "Query returned no results"}
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
            return result["text"]

        if result["type"] == "error":
            return f"Error: {result['message']}"

        if result["type"] == "modify":
            return f"Query executed successfully. Rows affected: {result['rowcount']}"

        columns = result["columns"]
        rows = result["rows"]
        has_more = result["has_more"]
        formatted = format_results(columns, rows, format)

        if format == "json":
            meta = {
                "data": json.loads(formatted),
                "row_count": len(rows),
                "truncated": has_more,
                "execution_time_ms": round(elapsed_ms, 2),
            }
            return json.dumps(meta, indent=2, ensure_ascii=False)

        return formatted

    except Error as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return f"Error executing query: {str(e)}"


@mcp.tool()
async def describe_table(
    table: Annotated[str, "The table name to describe"],
) -> str:
    """Get table schema information including column names, types, and constraints."""
    state = get_state()
    table = validate_table_name(table)

    def _sync_describe():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table}")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return format_results(columns, rows, "markdown")

    try:
        return await asyncio.to_thread(_sync_describe)
    except Error as e:
        logger.error(f"Error describing table '{table}': {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def health_check() -> str:
    """Check GreptimeDB connection status and server version."""
    state = get_state()
    start_time = time.time()

    def _sync_health_check():
        with state.get_connection() as conn:
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
            "host": state.db_config["host"],
            "port": state.db_config["port"],
            "database": state.db_config["database"],
            "version": version,
            "response_time_ms": round(elapsed_ms, 2),
        }
        return json.dumps(result, indent=2)

    except Error as e:
        logger.error(f"Health check failed: {e}")
        result = {
            "status": "unhealthy",
            "error": str(e),
            "host": state.db_config["host"],
            "port": state.db_config["port"],
        }
        return json.dumps(result, indent=2)


@mcp.tool()
async def execute_tql(
    query: Annotated[str, "PromQL expression, e.g., rate(http_requests_total[5m])"],
    start: Annotated[
        str, "Start time (RFC3339, Unix timestamp, or relative like 'now-1h')"
    ],
    end: Annotated[str, "End time (RFC3339, Unix timestamp, or relative like 'now')"],
    step: Annotated[str, "Query resolution step, e.g., '1m', '5m', '1h'"],
    lookback: Annotated[str | None, "Lookback delta for range queries"] = None,
) -> str:
    """Execute TQL (PromQL-compatible) query for time-series analysis."""
    state = get_state()

    if not all([query, start, end, step]):
        raise ValueError("query, start, end, and step are required")

    validate_tql_param(start, "start")
    validate_tql_param(end, "end")
    validate_tql_param(step, "step")
    if lookback:
        validate_tql_param(lookback, "lookback")

    is_dangerous, reason = security_gate(query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    if lookback:
        tql = f"TQL EVAL ('{start}', '{end}', '{step}', '{lookback}') {query}"
    else:
        tql = f"TQL EVAL ('{start}', '{end}', '{step}') {query}"

    start_time = time.time()

    def _sync_tql():
        with state.get_connection() as conn:
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
        return json.dumps(meta, indent=2, ensure_ascii=False)

    except Error as e:
        logger.error(f"Error executing TQL '{tql}': {e}")
        return f"Error executing TQL: {str(e)}"


@mcp.tool()
async def query_range(
    table: Annotated[str, "Table name to query"],
    select: Annotated[
        str, "Columns and aggregations, e.g., 'ts, host, avg(cpu) RANGE \\'5m\\''"
    ],
    align: Annotated[str, "Alignment interval, e.g., '1m', '5m'"],
    by: Annotated[str | None, "Group by columns, e.g., 'host'"] = None,
    where: Annotated[str | None, "WHERE clause conditions"] = None,
    fill: Annotated[str | None, "Fill strategy: NULL, PREV, LINEAR, or a value"] = None,
    order_by: Annotated[str, "ORDER BY clause"] = "ts DESC",
    limit: Annotated[int, "Maximum rows to return"] = 1000,
) -> str:
    """Execute time-window aggregation query using GreptimeDB's RANGE query syntax."""
    state = get_state()

    if not all([table, select, align]):
        raise ValueError("table, select, and align are required")

    validate_table_name(table)
    validate_duration(align, "align")
    validate_fill(fill)
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

    if fill:
        query_parts.append(f"FILL {fill}")

    query_parts.append(f"ORDER BY {order_by}")
    query_parts.append(f"LIMIT {limit}")

    query = " ".join(query_parts)

    is_dangerous, reason = security_gate(query=query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    start_time = time.time()

    def _sync_range():
        with state.get_connection() as conn:
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
        return json.dumps(meta, indent=2, ensure_ascii=False)

    except Error as e:
        logger.error(f"Error executing range query '{query}': {e}")
        return f"Error executing range query: {str(e)}"


@mcp.tool()
async def explain_query(
    query: Annotated[str, "SQL or TQL query to analyze"],
    analyze: Annotated[bool, "Execute and show actual metrics"] = False,
) -> str:
    """Analyze SQL or TQL query execution plan."""
    state = get_state()

    if not query:
        raise ValueError("query is required")

    is_dangerous, reason = security_gate(query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    if query.strip().upper().startswith("TQL"):
        # Replace TQL EVAL or TQL EVALUATE at start with TQL ANALYZE/EXPLAIN
        replacement = "TQL ANALYZE" if analyze else "TQL EXPLAIN"
        explain_query_str = re.sub(
            r"^\s*TQL\s+(EVAL(UATE)?)",
            replacement,
            query,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        if analyze:
            explain_query_str = f"EXPLAIN ANALYZE {query}"
        else:
            explain_query_str = f"EXPLAIN {query}"

    def _sync_explain():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(explain_query_str)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return format_results(columns, rows, "markdown")

    try:
        return await asyncio.to_thread(_sync_explain)
    except Error as e:
        logger.error(f"Error explaining query '{query}': {e}")
        return f"Error explaining query: {str(e)}"


@mcp.resource("greptime://{table}/data")
async def read_table_resource(table: str) -> str:
    """Read table contents (limited to 100 rows)."""
    state = get_state()
    table = validate_table_name(table)

    def _sync_read_table():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table} LIMIT %s", (RESULTS_LIMIT,))
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return format_results(columns, rows, "csv")

    try:
        return await asyncio.to_thread(_sync_read_table)
    except Error as e:
        logger.error(f"Database error reading table {table}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")


def _register_prompts():
    """Register prompts from templates."""
    templates = templates_loader()

    for name, template_data in templates.items():
        config = template_data["config"]
        template_content = template_data["template"]
        description = config.get("description", f"Prompt: {name}")

        # Extract argument names from config
        arg_names = [
            arg["name"]
            for arg in config.get("arguments", [])
            if isinstance(arg, dict) and "name" in arg
        ]

        # Create a prompt function dynamically
        def make_prompt_fn(tpl_content, arg_list):
            def prompt_fn(**kwargs) -> str:
                result = tpl_content
                for key, value in kwargs.items():
                    result = result.replace(f"{{{{ {key} }}}}", str(value))
                return result

            # Set function signature for FastMCP to detect arguments
            prompt_fn.__annotations__ = {arg: str for arg in arg_list}
            prompt_fn.__annotations__["return"] = str
            return prompt_fn

        prompt_fn = make_prompt_fn(template_content, arg_names)
        prompt_fn.__doc__ = description
        prompt_fn.__name__ = name

        # Use the decorator to register the prompt
        decorated = mcp.prompt(name=name, description=description)(prompt_fn)


# Register prompts at module load
_register_prompts()


def main():
    """Main entry point."""
    mcp.run()


if __name__ == "__main__":
    main()
