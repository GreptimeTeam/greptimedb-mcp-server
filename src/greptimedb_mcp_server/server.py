"""GreptimeDB MCP Server using FastMCP API."""

import asyncio
import sys

# Windows: use SelectorEventLoop for HTTP transports (signal handling),
# but keep ProactorEventLoop for stdio (pipe I/O support)
if sys.platform == "win32" and any(t in sys.argv for t in ("sse", "streamable-http")):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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
    audit_log,
    render_prompt_template,
)

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
from mcp.server.fastmcp.server import TransportSecuritySettings
from mysql.connector import connect, Error
from mysql.connector.pooling import MySQLConnectionPool

# Constants
RES_PREFIX = "greptime://"
RESULTS_LIMIT = 100
MAX_QUERY_LIMIT = 10000
MAX_SAMPLE_LIMIT = 20

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
    allow_write: bool = False
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


def _split_table_reference(table: str, default_schema: str) -> tuple[str, str]:
    """Split a possibly-qualified table reference into (schema, table).

    Mirrors GreptimeDB's table_idents_to_full_name: the table name is always the
    last segment, the schema the second-to-last. A leading catalog segment
    (catalog.schema.table) is accepted for compatibility but ignored, since
    information_schema is scoped to the connected catalog. Input is restricted to
    at most three unquoted segments by validate_table_name.
    """
    parts = table.split(".")
    if len(parts) == 1:
        return default_schema, parts[0]
    return parts[-2], parts[-1]


def _quote_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _quote_schema_table(table_schema: str, table_name: str) -> str:
    return f"{_quote_identifier(table_schema)}.{_quote_identifier(table_name)}"


def _normalize_nullable(value) -> bool | None:
    if value is None:
        return None
    text = str(value).upper()
    if text in {"YES", "Y", "TRUE", "1"}:
        return True
    if text in {"NO", "N", "FALSE", "0"}:
        return False
    return None


def _clean_comment(value) -> str | None:
    """Normalize a comment to a non-empty trimmed string, or None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _fetch_table_schema(cursor, table_schema: str, table_name: str) -> dict:
    """Read column-level schema, deriving the time index and primary keys."""
    cursor.execute(
        """
        SELECT column_name, data_type, semantic_type, is_nullable, column_comment
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_schema, table_name),
    )
    rows = cursor.fetchall()
    columns = []
    time_index = None
    primary_keys = []
    for row in rows:
        column_name, data_type, semantic_type, is_nullable = row[:4]
        column_comment = row[4] if len(row) > 4 else None
        semantic_type_text = str(semantic_type).upper() if semantic_type else ""
        if semantic_type_text == "TIMESTAMP":
            time_index = column_name
        elif semantic_type_text in {"TAG", "PRIMARY KEY", "PRIMARY_KEY"}:
            primary_keys.append(column_name)
        column = {
            "name": column_name,
            "data_type": data_type,
            "semantic_type": semantic_type,
            "nullable": _normalize_nullable(is_nullable),
        }
        comment = _clean_comment(column_comment)
        if comment:
            column["comment"] = comment
        columns.append(column)
    return {
        "columns": columns,
        "time_index": time_index,
        "primary_keys": primary_keys,
    }


def _fetch_table_comment(cursor, table_schema: str, table_name: str) -> str | None:
    """Read the table-level comment, returning None when absent or empty."""
    cursor.execute(
        """
        SELECT table_comment
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (table_schema, table_name),
    )
    rows = cursor.fetchall()
    if rows:
        return _clean_comment(rows[0][0])
    return None


def _fetch_table_semantics(cursor, table_schema: str, table_name: str) -> dict:
    """Read the experimental table_semantics view, degrading if it is absent."""
    try:
        cursor.execute(
            """
            SELECT table_catalog, table_schema, table_name, table_id,
                   signal_type, source, pipeline, metadata_quality,
                   semantic_options
            FROM information_schema.table_semantics
            WHERE table_schema = %s AND table_name = %s
            """,
            (table_schema, table_name),
        )
        # fetchall() drains the unbuffered cursor before the next query runs;
        # the WHERE clause matches at most one row.
        rows = cursor.fetchall()
        row = rows[0] if rows else None
    except Error as e:
        return {
            "included": True,
            "available": False,
            "found": False,
            "error": str(e),
        }

    if not row:
        return {"included": True, "available": True, "found": False}

    semantic_options = row[8]
    options = {}
    raw_options = None
    parse_error = None
    if semantic_options:
        try:
            parsed = json.loads(semantic_options)
        except (TypeError, json.JSONDecodeError) as e:
            raw_options = semantic_options
            parse_error = str(e)
        else:
            # Guidance treats options as a key/value map; a non-object payload
            # would crash _build_table_guidance, so keep it as raw instead.
            if isinstance(parsed, dict):
                options = parsed
            else:
                raw_options = semantic_options
                parse_error = "semantic_options is not a JSON object"

    result = {
        "included": True,
        "available": True,
        "found": True,
        "table_catalog": row[0],
        "table_schema": row[1],
        "table_name": row[2],
        "table_id": row[3],
        "signal_type": row[4],
        "source": row[5],
        "pipeline": row[6],
        "metadata_quality": row[7],
        "options": options,
    }
    if raw_options is not None:
        result["raw_options"] = raw_options
        result["options_parse_error"] = parse_error
    return result


def _fetch_table_samples(
    cursor, state, table_schema: str, table_name: str, schema: dict, sample_limit: int
) -> dict:
    """Read a small sample, preferring the newest rows when a time index exists.

    The sample query targets the resolved schema.table, ignoring any catalog
    segment, to stay consistent with how schema/semantics are resolved.
    """
    if sample_limit == 0:
        return {"included": True, "limit": 0, "columns": [], "rows": []}

    try:
        quoted_table = _quote_schema_table(table_schema, table_name)
        time_index = schema.get("time_index")
        if time_index:
            strategy = "latest_by_time_index"
            query = (
                f"SELECT * FROM {quoted_table} "
                f"ORDER BY {_quote_identifier(time_index)} DESC LIMIT %s"
            )
        else:
            strategy = "plain_limit"
            query = f"SELECT * FROM {quoted_table} LIMIT %s"

        cursor.execute(query, (sample_limit,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        formatted = format_results(
            columns,
            rows,
            "json",
            mask_enabled=state.mask_enabled,
            mask_patterns=state.mask_patterns,
        )
        return {
            "included": True,
            "strategy": strategy,
            "limit": sample_limit,
            "columns": columns,
            "rows": json.loads(formatted),
        }
    except Exception as e:
        # Best-effort sample: a failed read (missing table, unsupported value
        # type) must not sink the rest of the profile.
        return {
            "included": True,
            "limit": sample_limit,
            "columns": [],
            "rows": [],
            "error": str(e),
        }


def _build_table_guidance(schema: dict, semantics: dict, samples: dict) -> list[str]:
    """Derive query hints from signal type, metric kind, and sample ordering."""
    guidance = []
    if semantics.get("included") and not semantics.get("available", True):
        guidance.append(
            "Table semantic metadata is unavailable. The connected GreptimeDB "
            "version may not support information_schema.table_semantics."
        )
    elif semantics.get("included") and not semantics.get("found"):
        guidance.append(
            "No table semantic metadata was found. Treat signal type and "
            "query pattern as schema/sample-based inference."
        )

    signal_type = semantics.get("signal_type")
    options = semantics.get("options") or {}
    metric_type = options.get("metric.type")
    metadata_quality = semantics.get("metadata_quality")

    if signal_type == "metric":
        if metric_type == "counter":
            guidance.append(
                "This table is a counter metric. Prefer rate or increase "
                "queries for trend analysis."
            )
        elif metric_type == "gauge":
            guidance.append(
                "This table is a gauge metric. Prefer absolute value, avg, "
                "min, max, or percentile analysis."
            )
        elif metric_type == "histogram":
            guidance.append(
                "This table is a histogram metric. Prefer bucket/count/sum "
                "based percentile analysis."
            )
        if metadata_quality == "inferred":
            guidance.append(
                "Metric type was inferred from naming. Re-check the query "
                "choice if the metric name is non-standard."
            )
    elif signal_type == "trace":
        guidance.append(
            "This table represents traces. Prefer latency, error span, and "
            "service-level aggregation queries."
        )
    elif signal_type == "log":
        guidance.append(
            "This table represents logs. Prefer full-text search plus "
            "severity, time, and service aggregations."
        )

    if samples.get("included") and samples.get("strategy") == "latest_by_time_index":
        guidance.append(
            f"Sample rows are ordered by time index {schema.get('time_index')} descending."
        )
    return guidance


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
        allow_write=config.allow_write,
        http_session=aiohttp.ClientSession(),
    )

    logger.info(f"GreptimeDB Config: {db_config}")
    logger.info(f"Data masking: {'enabled' if config.mask_enabled else 'disabled'}")
    if config.allow_write:
        logger.warning(
            "Write mode ENABLED: execute_sql allows destructive SQL (DDL/DML). "
            "Do NOT use against production data."
        )
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
                if has_more:
                    # MySQL connector requires all results consumed before connection reuse
                    while cursor.fetchone():
                        pass
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
    """Execute SQL query against GreptimeDB. Please use MySQL dialect.

    Read-only by default. When the server runs with write mode enabled
    (--allow-write / GREPTIMEDB_ALLOW_WRITE), destructive SQL (DDL/DML) is
    also permitted.
    """
    state = get_state()
    limit = _validate_sql_params(query, format, limit)

    if not state.allow_write:
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
    table: Annotated[
        str,
        "Table name to describe (supports table, schema.table, or "
        "catalog.schema.table format)",
    ],
    include_semantics: Annotated[
        bool,
        "Include table semantic metadata from information_schema.table_semantics",
    ] = True,
    include_samples: Annotated[
        bool,
        "Include a small sample of table rows for context",
    ] = True,
    sample_limit: Annotated[
        int,
        f"Maximum sample rows to return (0-{MAX_SAMPLE_LIMIT}, default: 5)",
    ] = 5,
) -> str:
    """Get a table profile: schema, semantic metadata, sample rows, and guidance."""
    state = get_state()
    table = validate_table_name(table)
    sample_limit = max(0, min(sample_limit, MAX_SAMPLE_LIMIT))
    table_schema, table_name = _split_table_reference(
        table, state.db_config["database"]
    )

    def _sync_describe():
        with state.get_connection() as conn:
            with conn.cursor() as cursor:
                schema = _fetch_table_schema(cursor, table_schema, table_name)
                if not schema["columns"]:
                    return json.dumps(
                        {
                            "table": table,
                            "table_schema": table_schema,
                            "table_name": table_name,
                            "schema": schema,
                            "error": (
                                f"Table '{table}' not found or has no visible "
                                "columns."
                            ),
                        },
                        ensure_ascii=False,
                        indent=2,
                        default=str,
                    )
                table_comment = _fetch_table_comment(cursor, table_schema, table_name)
                semantics = (
                    _fetch_table_semantics(cursor, table_schema, table_name)
                    if include_semantics
                    else {"included": False}
                )
                samples = (
                    _fetch_table_samples(
                        cursor, state, table_schema, table_name, schema, sample_limit
                    )
                    if include_samples
                    else {"included": False}
                )
                result = {
                    "table": table,
                    "table_schema": table_schema,
                    "table_name": table_name,
                    **({"table_comment": table_comment} if table_comment else {}),
                    "schema": schema,
                    "semantics": semantics,
                    "samples": samples,
                    "guidance": _build_table_guidance(schema, semantics, samples),
                }
                return json.dumps(result, ensure_ascii=False, indent=2, default=str)

    try:
        return await asyncio.to_thread(_sync_describe)
    except Error as e:
        logger.error(f"Error describing table '{table}': {e}")
        return json.dumps(
            {
                "table": table,
                "table_schema": table_schema,
                "table_name": table_name,
                "error": f"Error describing table: {str(e)}",
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )


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
        "End time: SQL expression (e.g., 'now()'), RFC3339, or Unix timestamp",
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
                        f"Pipeline '{name}' created successfully.\nVersion: {version}"
                    )
                except (json.JSONDecodeError, KeyError, IndexError):
                    return f"Pipeline '{name}' created successfully."
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error creating pipeline (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error creating pipeline '{name}': {e}")
        return f"Error creating pipeline: {str(e)}"


@mcp.tool()
async def dryrun_pipeline(
    pipeline: Annotated[
        str | None,
        "Pipeline configuration in YAML format (inline). Provide this to test a pipeline without saving it.",
    ] = None,
    pipeline_name: Annotated[
        str | None,
        "Name of the saved pipeline to test. Provide either 'pipeline' or 'pipeline_name', not both.",
    ] = None,
    data: Annotated[
        str, "Test data in JSON or NDJSON format (single object or array)"
    ] = "",
    data_type: Annotated[
        str | None,
        "Content type of the data (e.g., 'application/x-ndjson'). If omitted, GreptimeDB will use default.",
    ] = None,
) -> str:
    """Test a pipeline with sample data without writing to the database.

    You can test a pipeline in two ways:
    - Provide 'pipeline' with inline YAML configuration
    - Provide 'pipeline_name' to test a previously saved pipeline

    Args:
        pipeline: Pipeline YAML configuration (inline)
        pipeline_name: Name of saved pipeline (mutually exclusive with pipeline)
        data: Test data in JSON/NDJSON format
        data_type: Optional content type (e.g., 'application/x-ndjson')
    """
    state = get_state()

    if not data or not data.strip():
        return "Error: data parameter is required"

    if pipeline is not None and pipeline_name is not None:
        return "Error: Provide either 'pipeline' or 'pipeline_name', not both"

    if pipeline is None and pipeline_name is None:
        return "Error: Provide either 'pipeline' or 'pipeline_name'"

    if pipeline_name is not None:
        pipeline_name = _validate_pipeline_name(pipeline_name)

    url = f"{state.http_base_url}/v1/pipelines/_dryrun"
    request_body = {"data": data}

    if data_type:
        request_body["data_type"] = data_type

    if pipeline is not None:
        request_body["pipeline"] = pipeline
    elif pipeline_name is not None:
        request_body["pipeline_name"] = pipeline_name

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
                    f"Error testing pipeline (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error testing pipeline: {e}")
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
                    f"Error deleting pipeline (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error deleting pipeline '{name}': {e}")
        return f"Error deleting pipeline: {str(e)}"


DASHBOARD_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_\-]*$")


def _validate_dashboard_name(name: str) -> str:
    """Validate dashboard name format."""
    if not name:
        raise ValueError("Dashboard name is required")
    if not DASHBOARD_NAME_PATTERN.match(name):
        raise ValueError(
            "Invalid dashboard name: must start with letter or underscore, "
            "contain only alphanumeric characters, underscores, and hyphens"
        )
    return name


@mcp.tool()
async def list_dashboards() -> str:
    """List all Perses dashboard definitions stored in GreptimeDB."""
    state = get_state()
    url = f"{state.http_base_url}/v1/dashboards"
    auth = state.get_http_auth()

    try:
        async with state.http_session.get(url, auth=auth) as response:
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
                    f"Error listing dashboards (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error listing dashboards: {e}")
        return f"Error listing dashboards: {str(e)}"


@mcp.tool()
async def create_dashboard(
    name: Annotated[str, "Name of the dashboard"],
    definition: Annotated[str, "Perses dashboard definition in JSON format"],
) -> str:
    """Create or update a Perses dashboard definition in GreptimeDB."""
    state = get_state()
    name = _validate_dashboard_name(name)

    url = f"{state.http_base_url}/v1/dashboards/{quote(name)}"
    auth = state.get_http_auth()

    try:
        json_definition = json.loads(definition)
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON definition: {str(e)}"

    try:
        async with state.http_session.post(
            url,
            json=json_definition,
            auth=auth,
        ) as response:
            response_text = await response.text()

            if response.status == 200:
                return f"Dashboard '{name}' saved successfully."
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error creating dashboard (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error creating dashboard '{name}': {e}")
        return f"Error creating dashboard: {str(e)}"


@mcp.tool()
async def delete_dashboard(
    name: Annotated[str, "Name of the dashboard to delete"],
) -> str:
    """Delete a Perses dashboard definition from GreptimeDB."""
    state = get_state()
    name = _validate_dashboard_name(name)

    url = f"{state.http_base_url}/v1/dashboards/{quote(name)}"
    auth = state.get_http_auth()

    try:
        async with state.http_session.delete(url, auth=auth) as response:
            response_text = await response.text()

            if response.status == 200:
                return f"Dashboard '{name}' deleted successfully."
            else:
                error_detail = response_text if response_text else "No details"
                return (
                    f"Error deleting dashboard (HTTP {response.status}): {error_detail}"
                )

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error deleting dashboard '{name}': {e}")
        return f"Error deleting dashboard: {str(e)}"


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
            (
                f"{arg_name}: Annotated[str, {repr(arg_desc)}]"
                if required
                else f"{arg_name}: Annotated[str, {repr(arg_desc)}] = ''"
            )
            for arg_name, arg_desc, required in arg_info
        )

        arg_tuples = ", ".join(f'("{n}", {n})' for n, _, _ in arg_info)
        if arg_params:
            arg_params = f"*, {arg_params}"

        func_code = f"""
def prompt_fn({arg_params}) -> str:
    context = dict([{arg_tuples}])
    return render_prompt_template(template_content, context)
"""
        namespace = {
            "template_content": template_content,
            "Annotated": Annotated,
            "render_prompt_template": render_prompt_template,
        }
        exec(func_code, namespace)
        prompt_fn = namespace["prompt_fn"]
        prompt_fn.__doc__ = description
        prompt_fn.__name__ = name
        mcp.prompt(name=name, description=description)(prompt_fn)


# Register prompts at module load
_register_prompts()


def _install_audit_hook():
    """Install audit logging hook by wrapping tool manager's call_tool method."""
    original_call_tool = mcp._tool_manager.call_tool

    async def audited_call_tool(name, arguments, context=None, convert_result=False):
        start_time = time.time()
        try:
            result = await original_call_tool(name, arguments, context, convert_result)
            elapsed_ms = (time.time() - start_time) * 1000
            audit_log(name, arguments, success=True, duration_ms=elapsed_ms)
            return result
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            audit_log(
                name, arguments, success=False, duration_ms=elapsed_ms, error=str(e)
            )
            raise

    mcp._tool_manager.call_tool = audited_call_tool


def main():
    """Main entry point."""
    global _config
    _config = Config.from_env_arguments()

    # Install audit logging hook if enabled
    if _config.audit_enabled:
        _install_audit_hook()
        logger.info("Audit logging: enabled")
    else:
        logger.info("Audit logging: disabled")

    # Only configure HTTP server settings for non-stdio transports
    # to avoid overriding user's programmatic configuration
    if _config.transport != "stdio":
        mcp.settings.host = _config.listen_host
        mcp.settings.port = _config.listen_port

        # Configure DNS rebinding protection
        # If allowed_hosts is empty, disable protection for compatibility
        # with proxies, gateways, and Kubernetes services
        if _config.allowed_hosts:
            security_kwargs = {
                "enable_dns_rebinding_protection": True,
                "allowed_hosts": _config.allowed_hosts,
            }
            if _config.allowed_origins:
                security_kwargs["allowed_origins"] = _config.allowed_origins
            mcp.settings.transport_security = TransportSecuritySettings(
                **security_kwargs
            )
            logger.info(
                f"DNS rebinding protection: enabled "
                f"(allowed_hosts: {_config.allowed_hosts}, "
                f"allowed_origins: {_config.allowed_origins or 'default'})"
            )
        else:
            mcp.settings.transport_security = TransportSecuritySettings(
                enable_dns_rebinding_protection=False,
            )
            logger.info("DNS rebinding protection: disabled")

        logger.info(
            f"Starting MCP server with transport: {_config.transport} "
            f"on {_config.listen_host}:{_config.listen_port}"
        )
    else:
        logger.info("Starting MCP server with transport: stdio")

    mcp.run(transport=_config.transport)


if __name__ == "__main__":
    main()
