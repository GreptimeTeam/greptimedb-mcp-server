"""GreptimeDB MCP Server using FastMCP API."""

from greptimedb_mcp_server.config import Config
from greptimedb_mcp_server.formatter import format_results, VALID_FORMATS
from greptimedb_mcp_server.utils import (
    security_gate,
    templates_loader,
    validate_table_name,
    validate_tql_param,
    validate_query_component,
    validate_duration,
    validate_fill,
    validate_time_expression,
    format_tql_time_param,
)

import asyncio
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated
from urllib.parse import quote

import aiohttp
from mcp.server.fastmcp import FastMCP
from mysql.connector import connect, Error
from mysql.connector.pooling import MySQLConnectionPool

# Constants
RES_PREFIX = "greptime://"
RESULTS_LIMIT = 100
MAX_QUERY_LIMIT = 10000

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("greptimedb_mcp_server")


@dataclass
class AppState:
    """Application state shared across tools."""

    db_config: dict
    pool_config: dict
    templates: dict
    http_base_url: str
    mask_enabled: bool = True
    mask_patterns: list[str] = field(default_factory=list)
    pool: MySQLConnectionPool | None = field(default=None)
    http_session: aiohttp.ClientSession | None = field(default=None)

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

    def get_http_auth(self) -> aiohttp.BasicAuth | None:
        """Get HTTP Basic Auth if credentials are configured."""
        user = self.db_config.get("user", "")
        password = self.db_config.get("password", "")
        if user:
            return aiohttp.BasicAuth(user, password)
        return None


# Global config (set by main() before run())
_config: Config | None = None

# Global state (initialized in lifespan)
_state: AppState | None = None


def get_config() -> Config:
    """Get the parsed configuration.

    Falls back to parsing from env/args if not pre-initialized by main().
    This preserves compatibility with alternative entry points like
    `mcp dev greptimedb_mcp_server.server:mcp` or programmatic imports.
    """
    global _config
    if _config is None:
        _config = Config.from_env_arguments()
    return _config


def get_state() -> AppState:
    """Get the application state."""
    if _state is None:
        raise RuntimeError("Application state not initialized")
    return _state


@asynccontextmanager
async def lifespan(mcp: FastMCP):
    """Initialize application state on startup."""
    global _state

    config = get_config()
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
        "pool_size": config.pool_size,
        "pool_reset_session": True,
        **db_config,
    }

    # Parse mask_patterns from comma-separated string
    mask_patterns = []
    if config.mask_patterns:
        mask_patterns = [
            p.strip() for p in config.mask_patterns.split(",") if p.strip()
        ]

    http_base_url = f"{config.http_protocol}://{config.host}:{config.http_port}"

    _state = AppState(
        db_config=db_config,
        pool_config=pool_config,
        templates=templates_loader(),
        http_base_url=http_base_url,
        mask_enabled=config.mask_enabled,
        mask_patterns=mask_patterns,
        http_session=aiohttp.ClientSession(),
    )

    logger.info(f"GreptimeDB Config: {db_config}")
    logger.info(f"Data masking: {'enabled' if config.mask_enabled else 'disabled'}")
    logger.info("Starting GreptimeDB MCP server...")

    try:
        yield _state
    finally:
        logger.info("Shutting down GreptimeDB MCP server...")
        if _state.http_session:
            await _state.http_session.close()


mcp = FastMCP(
    "greptimedb_mcp_server",
    instructions="GreptimeDB MCP Server - provides secure read-only access to GreptimeDB",
    lifespan=lifespan,
)

# Query type constants
_READ_COMMANDS = ("SELECT", "SHOW", "DESC", "TQL", "EXPLAIN", "WITH")


def _process_query_result(result: dict, format: str, elapsed_ms: float) -> str:
    """Process and format query execution result."""
    if result["type"] == "simple":
        return result["text"]

    if result["type"] == "error":
        return f"Error: {result['message']}"

    if result["type"] == "modify":
        return f"Query executed successfully. Rows affected: {result['rowcount']}"

    # Handle query results
    state = get_state()
    formatted = format_results(
        result["columns"],
        result["rows"],
        format,
        mask_enabled=state.mask_enabled,
        mask_patterns=state.mask_patterns,
    )

    if format == "json":
        meta = {
            "data": json.loads(formatted),
            "row_count": len(result["rows"]),
            "truncated": result["has_more"],
            "execution_time_ms": round(elapsed_ms, 2),
        }
        return json.dumps(meta, indent=2, ensure_ascii=False)

    return formatted


def _validate_sql_params(query: str, format: str, limit: int) -> int:
    """Validate SQL parameters and return normalized limit."""
    if not query:
        raise ValueError("Query is required")
    if format not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format}. Must be one of: {VALID_FORMATS}")
    return min(max(1, limit), MAX_QUERY_LIMIT)


def _execute_query(state: AppState, query: str, limit: int) -> dict:
    """Execute query synchronously and return result dict."""
    with state.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            stmt = query.strip().upper()

            if stmt.startswith("SHOW DATABASES"):
                rows = cursor.fetchall()
                header = cursor.description[0][0] if cursor.description else "Database"
                return {
                    "type": "simple",
                    "text": header + "\n" + "\n".join(r[0] for r in rows),
                }

            if stmt.startswith("SHOW TABLES"):
                rows = cursor.fetchall()
                header = cursor.description[0][0] if cursor.description else "Tables"
                return {
                    "type": "simple",
                    "text": header + "\n" + "\n".join(r[0] for r in rows),
                }

            if any(stmt.startswith(cmd) for cmd in _READ_COMMANDS):
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
    limit = _validate_sql_params(query, format, limit)

    is_dangerous, reason = security_gate(query=query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    start_time = time.time()

    try:
        result = await asyncio.to_thread(_execute_query, state, query, limit)
        elapsed_ms = (time.time() - start_time) * 1000
        return _process_query_result(result, format, elapsed_ms)

    except Error as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return f"Error executing query: {str(e)}"


@mcp.tool()
async def describe_table(
    table: Annotated[str, "Table name to describe (supports schema.table format)"],
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
                return format_results(
                    columns,
                    rows,
                    "markdown",
                    mask_enabled=state.mask_enabled,
                    mask_patterns=state.mask_patterns,
                )

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
    query: Annotated[
        str,
        "PromQL-compatible expression. Supports standard PromQL syntax: "
        "rate(), increase(), sum(), avg(), histogram_quantile(), etc. "
        "Example: rate(http_requests_total[5m])",
    ],
    start: Annotated[
        str,
        "Start time: SQL expression (e.g., \"now() - interval '5' minute\"), "
        "RFC3339 (e.g., '2024-01-01T00:00:00Z'), or Unix timestamp",
    ],
    end: Annotated[
        str,
        "End time: SQL expression (e.g., 'now()'), " "RFC3339, or Unix timestamp",
    ],
    step: Annotated[str, "Query resolution step, e.g., '1m', '5m', '1h'"],
    lookback: Annotated[str | None, "Lookback delta for range queries"] = None,
    format: Annotated[
        str, "Output format: csv, json, or markdown (default: json)"
    ] = "json",
) -> str:
    """Execute TQL query for time-series analysis. TQL is PromQL-compatible - use standard PromQL syntax."""
    state = get_state()

    if not all([query, start, end, step]):
        raise ValueError("query, start, end, and step are required")
    if format not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format}. Must be one of: {VALID_FORMATS}")

    validate_time_expression(start, "start")
    validate_time_expression(end, "end")
    validate_tql_param(step, "step")
    if lookback:
        validate_tql_param(lookback, "lookback")

    is_dangerous, reason = security_gate(query)
    if is_dangerous:
        return f"Error: Dangerous operation blocked: {reason}"

    start_param = format_tql_time_param(start)
    end_param = format_tql_time_param(end)
    if lookback:
        tql = f"TQL EVAL ({start_param}, {end_param}, '{step}', '{lookback}') {query}"
    else:
        tql = f"TQL EVAL ({start_param}, {end_param}, '{step}') {query}"

    start_time = time.time()

    def _sync_tql():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(tql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchmany(MAX_QUERY_LIMIT)
                return columns, rows

    try:
        columns, rows = await asyncio.to_thread(_sync_tql)
        elapsed_ms = (time.time() - start_time) * 1000
        formatted = format_results(
            columns,
            rows,
            format,
            mask_enabled=state.mask_enabled,
            mask_patterns=state.mask_patterns,
        )

        if format == "json":
            meta = {
                "tql": tql,
                "data": json.loads(formatted),
                "row_count": len(rows),
                "execution_time_ms": round(elapsed_ms, 2),
            }
            return json.dumps(meta, indent=2, ensure_ascii=False)

        return formatted

    except Error as e:
        logger.error(f"Error executing TQL '{tql}': {e}")
        return f"Error executing TQL: {str(e)}"


@mcp.tool()
async def query_range(
    table: Annotated[str, "Table name to query (supports schema.table format)"],
    select: Annotated[
        str, "Columns and aggregations, e.g., 'ts, host, avg(cpu) RANGE \\'5m\\''"
    ],
    align: Annotated[str, "Alignment interval, e.g., '1m', '5m'"],
    by: Annotated[str | None, "Group by columns, e.g., 'host'"] = None,
    where: Annotated[str | None, "WHERE clause conditions"] = None,
    fill: Annotated[str | None, "Fill strategy: NULL, PREV, LINEAR, or a value"] = None,
    order_by: Annotated[str | None, "ORDER BY clause (e.g., 'ts DESC')"] = None,
    format: Annotated[
        str, "Output format: csv, json, or markdown (default: json)"
    ] = "json",
    limit: Annotated[int, "Maximum rows to return"] = 1000,
) -> str:
    """Execute time-window aggregation query using GreptimeDB's RANGE query syntax."""
    state = get_state()

    if not all([table, select, align]):
        raise ValueError("table, select, and align are required")
    if format not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format}. Must be one of: {VALID_FORMATS}")

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

    if order_by:
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
        formatted = format_results(
            columns,
            rows,
            format,
            mask_enabled=state.mask_enabled,
            mask_patterns=state.mask_patterns,
        )

        if format == "json":
            meta = {
                "query": query,
                "data": json.loads(formatted),
                "row_count": len(rows),
                "execution_time_ms": round(elapsed_ms, 2),
            }
            return json.dumps(meta, indent=2, ensure_ascii=False)

        return formatted

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
                return format_results(
                    columns,
                    rows,
                    "markdown",
                    mask_enabled=state.mask_enabled,
                    mask_patterns=state.mask_patterns,
                )

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
                return format_results(
                    columns,
                    rows,
                    "csv",
                    mask_enabled=state.mask_enabled,
                    mask_patterns=state.mask_patterns,
                )

    try:
        return await asyncio.to_thread(_sync_read_table)
    except Error as e:
        logger.error(f"Database error reading table {table}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")


PIPELINE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_pipeline_name(name: str) -> str:
    """Validate pipeline name format."""
    if not name:
        raise ValueError("Pipeline name is required")
    if not PIPELINE_NAME_PATTERN.match(name):
        raise ValueError(
            "Invalid pipeline name: must start with letter or underscore, "
            "contain only alphanumeric characters and underscores"
        )
    return name


def _format_pipeline_version(ns_timestamp: int) -> str:
    """Convert nanosecond timestamp to HTTP API version format (UTC)."""
    seconds = ns_timestamp // 1_000_000_000
    nanoseconds = ns_timestamp % 1_000_000_000
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{nanoseconds:09d}"


@mcp.tool()
async def list_pipelines(
    name: Annotated[str | None, "Optional pipeline name to filter by"] = None,
) -> str:
    """List all pipelines or get details of a specific pipeline."""
    state = get_state()

    if name:
        query = (
            "SELECT name, pipeline, created_at::bigint as version "
            "FROM greptime_private.pipelines WHERE name = %s"
        )
        params = (name,)
    else:
        query = (
            "SELECT name, pipeline, created_at::bigint as version "
            "FROM greptime_private.pipelines"
        )
        params = ()

    def _sync_list():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return columns, rows

    try:
        columns, rows = await asyncio.to_thread(_sync_list)
        if not rows:
            return "No pipelines found."

        version_idx = columns.index("version")
        converted_rows = []
        for row in rows:
            row_list = list(row)
            if row_list[version_idx] is not None:
                row_list[version_idx] = _format_pipeline_version(row_list[version_idx])
            converted_rows.append(tuple(row_list))

        result = format_results(
            columns,
            converted_rows,
            "markdown",
            mask_enabled=False,
            mask_patterns=[],
        )
        return result

    except Error as e:
        logger.error(f"Error listing pipelines: {e}")
        return f"Error listing pipelines: {str(e)}"


@mcp.tool()
async def create_pipeline(
    name: Annotated[str, "Name of the pipeline to create"],
    pipeline: Annotated[str, "Pipeline configuration in YAML format"],
) -> str:
    """Create a new pipeline in GreptimeDB."""
    state = get_state()
    name = _validate_pipeline_name(name)

    url = f"{state.http_base_url}/v1/pipelines/{quote(name)}"
    auth = state.get_http_auth()

    try:
        async with state.http_session.post(
            url,
            data=pipeline,
            headers={"Content-Type": "application/x-yaml"},
            auth=auth,
        ) as response:
            response_text = await response.text()

            if response.status == 200:
                try:
                    result = json.loads(response_text)
                    pipelines = result.get("pipelines", [])
                    version = pipelines[0]["version"] if pipelines else "unknown"
                    return (
                        f"Pipeline '{name}' created successfully.\n"
                        f"Version: {version}"
                    )
                except (json.JSONDecodeError, KeyError, IndexError):
                    return f"Pipeline '{name}' created successfully."
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error creating pipeline (HTTP {response.status}): "
                    f"{error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error creating pipeline '{name}': {e}")
        return f"Error creating pipeline: {str(e)}"


@mcp.tool()
async def dryrun_pipeline(
    pipeline_name: Annotated[str, "Name of the pipeline to test"],
    data: Annotated[str, "Test data in JSON format (single object or array)"],
) -> str:
    """Test a pipeline with sample data without writing to the database."""
    state = get_state()
    pipeline_name = _validate_pipeline_name(pipeline_name)

    try:
        parsed = json.loads(data)
        normalized_data = json.dumps(parsed, ensure_ascii=False)
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON data: {str(e)}"

    url = f"{state.http_base_url}/v1/pipelines/_dryrun"
    request_body = {
        "pipeline_name": pipeline_name,
        "data": normalized_data,
    }
    auth = state.get_http_auth()
    logger.debug(f"Dryrun request URL: {url}")
    logger.debug(f"Dryrun request body: {request_body}")

    try:
        async with state.http_session.post(
            url,
            json=request_body,
            auth=auth,
        ) as response:
            response_text = await response.text()

            if response.status == 200:
                try:
                    result = json.loads(response_text)
                    return json.dumps(result, indent=2, ensure_ascii=False)
                except json.JSONDecodeError:
                    return response_text
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error testing pipeline (HTTP {response.status}): "
                    f"{error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error testing pipeline '{pipeline_name}': {e}")
        return f"Error testing pipeline: {str(e)}"


@mcp.tool()
async def delete_pipeline(
    name: Annotated[str, "Name of the pipeline to delete"],
    version: Annotated[str, "Version of the pipeline to delete (timestamp)"],
) -> str:
    """Delete a specific version of a pipeline from GreptimeDB."""
    state = get_state()
    name = _validate_pipeline_name(name)

    if not version:
        return "Error: version is required to delete a pipeline"

    url = f"{state.http_base_url}/v1/pipelines/{quote(name)}?version={quote(version)}"
    auth = state.get_http_auth()

    try:
        async with state.http_session.delete(url, auth=auth) as response:
            response_text = await response.text()

            if response.status == 200:
                return f"Pipeline '{name}' (version: {version}) deleted successfully."
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error deleting pipeline (HTTP {response.status}): "
                    f"{error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error deleting pipeline '{name}': {e}")
        return f"Error deleting pipeline: {str(e)}"


def _register_prompts():
    """Register prompts from templates."""
    templates = templates_loader()

    for name, template_data in templates.items():
        config = template_data["config"]
        template_content = template_data["template"]
        description = config.get("description", f"Prompt: {name}")

        args_config = config.get("arguments", [])
        arg_info = [
            (arg["name"], arg.get("description", ""), arg.get("required", False))
            for arg in args_config
            if isinstance(arg, dict) and "name" in arg
        ]

        invalid_args = [n for n, _, _ in arg_info if not n.isidentifier()]
        if invalid_args:
            logger.warning(
                f"Skipping prompt '{name}': invalid argument names {invalid_args}"
            )
            continue

        arg_params = ", ".join(
            f"{arg_name}: Annotated[str, {repr(arg_desc)}]"
            for arg_name, arg_desc, _ in arg_info
        )

        arg_tuples = ", ".join(f'("{n}", {n})' for n, _, _ in arg_info)
        func_code = f"""
def prompt_fn({arg_params}) -> str:
    result = template_content
    for key, value in [{arg_tuples}]:
        result = result.replace(f"{{{{{{{{ {{key}} }}}}}}}}", str(value))
    return result
"""
        namespace = {"template_content": template_content, "Annotated": Annotated}
        exec(func_code, namespace)
        prompt_fn = namespace["prompt_fn"]
        prompt_fn.__doc__ = description
        prompt_fn.__name__ = name
        mcp.prompt(name=name, description=description)(prompt_fn)


# Register prompts at module load
_register_prompts()


def main():
    """Main entry point."""
    global _config
    _config = Config.from_env_arguments()

    # Only configure HTTP server settings for non-stdio transports
    # to avoid overriding user's programmatic configuration
    if _config.transport != "stdio":
        mcp.settings.host = _config.listen_host
        mcp.settings.port = _config.listen_port
        logger.info(
            f"Starting MCP server with transport: {_config.transport} "
            f"on {_config.listen_host}:{_config.listen_port}"
        )
    else:
        logger.info("Starting MCP server with transport: stdio")

    mcp.run(transport=_config.transport)


if __name__ == "__main__":
    main()
