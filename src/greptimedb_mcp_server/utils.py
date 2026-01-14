import re
import logging
import yaml
import os
from typing import Any

logger = logging.getLogger("greptimedb_mcp_server")


def security_gate(query: str) -> tuple[bool, str]:
    """
    Simple security check for SQL queries.
    Args:
        query: The SQL query to check
    Returns:
        tuple: A boolean indicating if the query is dangerous, and a reason message
    """
    if not query or not query.strip():
        return True, "Empty query not allowed"

    # Check for encoded content before normalization (hex encoding bypass)
    if re.search(r"\b(?:UNHEX|0x[0-9a-fA-F]+|CHAR\s*\()", query, re.IGNORECASE):
        logger.warning(f"Encoded content detected: {query[:50]}...")
        return True, "Encoded query content not allowed"

    # Remove comments and normalize whitespace
    clean_query = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    clean_query = re.sub(r"--.*", "", clean_query)
    clean_query = re.sub(r"\s+", " ", clean_query).strip().upper()

    dangerous_patterns = [
        # DDL/DML operations (must be at start, not in SHOW CREATE TABLE etc.)
        (r"^\s*DROP\b", "Forbidden `DROP` operation"),
        (r"\bDELETE\b", "Forbidden `DELETE` operation"),
        (r"\bREVOKE\b", "Forbidden `REVOKE` operation"),
        (r"\bTRUNCATE\b", "Forbidden `TRUNCATE` operation"),
        (r"\bUPDATE\b", "Forbidden `UPDATE` operation"),
        (r"\bINSERT\b", "Forbidden `INSERT` operation"),
        (r"^\s*ALTER\b", "Forbidden `ALTER` operation"),
        (r"^\s*CREATE\b", "Forbidden `CREATE` operation"),
        (r"^\s*GRANT\b", "Forbidden `GRANT` operation"),
        # Dynamic SQL execution
        (r"\b(?:EXEC|EXECUTE)\b", "Dynamic SQL execution not allowed"),
        (r"\bCALL\b", "Stored procedure calls not allowed"),
        (r"\bREPLACE\s+INTO\b", "Forbidden `REPLACE INTO` operation"),
        # File system access
        (r"\bLOAD\b", "Forbidden `LOAD` operation"),
        (r"\bCOPY\b", "Forbidden `COPY` operation"),
        (r"\bOUTFILE\b", "Forbidden `OUTFILE` operation"),
        (r"\bLOAD_FILE\b", "Forbidden `LOAD_FILE` function"),
        (r"\bINTO\s+DUMPFILE\b", "Forbidden `INTO DUMPFILE` operation"),
        # Multiple statements (check for dangerous keywords after semicolon)
        (
            r";\s*(?:DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|GRANT|REVOKE|TRUNCATE)\b",
            "Forbidden multiple statements",
        ),
    ]

    for pattern, reason in dangerous_patterns:
        if re.search(pattern, clean_query):
            logger.warning(f"Dangerous pattern detected: {query[:50]}...")
            return True, reason

    return False, ""


def templates_loader() -> dict[str, dict[str, str]]:
    templates = {}
    template_dir = os.path.join(os.path.dirname(__file__), "templates")

    for category in os.listdir(template_dir):
        category_path = os.path.join(template_dir, category)
        if os.path.isdir(category_path):
            # Load config
            with open(
                os.path.join(category_path, "config.yaml"), "r", encoding="utf-8"
            ) as f:
                config = yaml.safe_load(f)

            # Load template
            with open(
                os.path.join(category_path, "template.md"), "r", encoding="utf-8"
            ) as f:
                template = f.read()

            templates[category] = {"config": config, "template": template}

    return templates


# Validation patterns
TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")
DURATION_PATTERN = re.compile(r"^(\d+)(ms|s|m|h|d|w|y)$")
FILL_PATTERN = re.compile(r"^(NULL|PREV|LINEAR|(-?\d+(\.\d+)?))$", re.IGNORECASE)


def validate_table_name(table: str) -> str:
    """Validate table name format. Supports schema.table format."""
    if not table:
        raise ValueError("Table name is required")
    if not TABLE_NAME_PATTERN.match(table):
        raise ValueError("Invalid table name: must be 'table' or 'schema.table' format")
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


def validate_duration(value: str, name: str) -> str:
    """Validate duration parameter follows Prometheus duration syntax."""
    if not value:
        raise ValueError(f"{name} is required")
    if not DURATION_PATTERN.match(value):
        raise ValueError(
            f"Invalid {name}: must be a duration like '1m', '5m', '1h', '30s'"
        )
    return value


def validate_fill(value: str) -> str:
    """Validate FILL parameter."""
    if not value:
        return value
    if not FILL_PATTERN.match(value):
        raise ValueError("Invalid fill: must be NULL, PREV, LINEAR, or a number")
    return value


def is_sql_time_expression(value: str) -> bool:
    """Check if value is a SQL time expression (contains function call)."""
    return "(" in value


def format_tql_time_param(value: str) -> str:
    """Format time parameter for TQL: quote literals, leave SQL expressions as-is."""
    if is_sql_time_expression(value):
        return value
    # Escape single quotes in literal values to avoid breaking the TQL statement
    safe_value = value.replace("'", "''")
    return f"'{safe_value}'"


def validate_time_expression(value: str, name: str) -> str:
    """Validate time expression for TQL start/end parameters."""
    if not value:
        raise ValueError(f"{name} is required")
    if ";" in value or "--" in value:
        raise ValueError(f"Invalid characters in {name}")
    if value.count("'") % 2 != 0:
        raise ValueError(f"Unbalanced quotes in {name}")
    is_dangerous, reason = security_gate(value)
    if is_dangerous:
        raise ValueError(f"Dangerous pattern in {name}: {reason}")
    return value


# Audit logging
audit_logger = logging.getLogger("greptimedb_mcp_server.audit")


def _truncate_value(v: Any, max_len: int = 200) -> str:
    """Truncate a value to max_len characters."""
    v_str = str(v)
    if len(v_str) > max_len:
        return v_str[:max_len] + "..."
    return v_str


def _format_audit_params(params: dict) -> str:
    """Format parameters for audit log."""
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append(f'{k}="{_truncate_value(v)}"')
    return " | ".join(parts)


def audit_log(
    tool: str,
    params: dict,
    success: bool,
    duration_ms: float,
    error: str | None = None,
):
    """Record audit log for tool invocation. Never raises exceptions."""
    try:
        parts = [f"[AUDIT] {tool}"]

        params_str = _format_audit_params(params)
        if params_str:
            parts.append(params_str)

        parts.append(f"success={success}")
        if error:
            parts.append(f'error="{_truncate_value(error)}"')
        parts.append(f"duration_ms={duration_ms:.1f}")

        audit_logger.info(" | ".join(parts))
    except Exception:
        pass
