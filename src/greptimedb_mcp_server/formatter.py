"""Result formatting utilities for GreptimeDB MCP Server."""

import csv
import datetime
import io
import json

VALID_FORMATS = {"csv", "json", "markdown"}


def _convert_value(val):
    """Convert datetime values to string."""
    if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
        return str(val)
    return val


def _escape_md(val) -> str:
    """Escape markdown special characters."""
    if val is None:
        return ""
    s = str(val)
    s = s.replace("\\", "\\\\")
    s = s.replace("|", "\\|")
    s = s.replace("\n", " ")
    s = s.replace("\r", "")
    return s


def _format_json(columns: list, rows: list) -> str:
    """Format results as JSON."""
    result = []
    for row in rows:
        row_dict = {col: _convert_value(row[i]) for i, col in enumerate(columns)}
        result.append(row_dict)
    return json.dumps(result, ensure_ascii=False, indent=2)


def _format_markdown(columns: list, rows: list) -> str:
    """Format results as markdown table."""
    escaped_cols = [_escape_md(c) for c in columns]
    header = "| " + " | ".join(escaped_cols) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"

    if not rows:
        return f"{header}\n{separator}"

    lines = [header, separator]
    for row in rows:
        formatted = [_escape_md(v) for v in row]
        lines.append("| " + " | ".join(formatted) + " |")
    return "\n".join(lines)


def _format_csv(columns: list, rows: list) -> str:
    """Format results as CSV."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_convert_value(v) for v in row])
    return output.getvalue().rstrip("\r\n")


def format_results(
    columns: list,
    rows: list,
    fmt: str = "csv",
    mask_enabled: bool = True,
    mask_patterns: list[str] | None = None,
) -> str:
    """Format query results in specified format.

    Args:
        columns: List of column names
        rows: List of row tuples
        fmt: Output format (csv, json, markdown)
        mask_enabled: Whether to mask sensitive columns
        mask_patterns: Additional sensitive patterns (combined with defaults)
    """
    # Apply masking if enabled
    if mask_enabled:
        from greptimedb_mcp_server.masking import (
            DEFAULT_SENSITIVE_PATTERNS,
            mask_rows,
        )

        patterns = list(DEFAULT_SENSITIVE_PATTERNS)
        if mask_patterns:
            patterns.extend(mask_patterns)
        rows = mask_rows(columns, rows, patterns)

    if fmt == "json":
        return _format_json(columns, rows)
    elif fmt == "markdown":
        return _format_markdown(columns, rows)
    else:
        return _format_csv(columns, rows)
