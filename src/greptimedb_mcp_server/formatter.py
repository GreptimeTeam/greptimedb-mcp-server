"""Result formatting utilities for GreptimeDB MCP Server."""

import csv
import datetime
import io
import json

VALID_FORMATS = {"csv", "json", "markdown"}

TRUNCATION_NOTICE = (
    "\n\n[truncated: the result was {actual} bytes, over the {budget}-byte "
    "budget, and was cut here. It is no longer valid {fmt}. Narrow the query, "
    "lower `limit`, or select fewer columns.]"
)


def truncate_to_budget(text: str, budget: int, fmt: str = "output") -> str:
    """Cut a rendered result down to a byte budget, saying so in the result.

    A result over the client's limit is rejected whole, so a cut result with a
    notice beats a call that returns nothing. Cutting mid-structure leaves
    invalid JSON or a torn CSV row, which is why the notice names the format:
    a caller that cannot parse the remainder still reads why and what to do.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= budget:
        return text

    notice = TRUNCATION_NOTICE.format(actual=len(encoded), budget=budget, fmt=fmt)
    keep = max(0, budget - len(notice.encode("utf-8")))
    # errors="ignore" drops a multi-byte character the cut landed inside.
    return encoded[:keep].decode("utf-8", errors="ignore") + notice


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
    # default=str keeps non-JSON-native values (Decimal, bytes, UUID) from
    # raising TypeError; they fall back to their string form.
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


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
