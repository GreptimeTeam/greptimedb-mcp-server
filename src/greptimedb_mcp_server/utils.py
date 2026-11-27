import re
import logging
import yaml
import os
from functools import lru_cache
from typing import Any
from jinja2 import StrictUndefined, Template, TemplateError
from jinja2.sandbox import SandboxedEnvironment

logger = logging.getLogger("greptimedb_mcp_server")

_PROMPT_TEMPLATE_ENV = SandboxedEnvironment(
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
    undefined=StrictUndefined,
)
_PROMPT_TEMPLATE_ENV.globals.clear()


# Statements the read-only gate lets through. EXPLAIN is judged by the
# statement it wraps, because EXPLAIN ANALYZE executes it.
READ_STATEMENT_KEYWORDS = frozenset(
    {"SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "TQL"}
)

# Clauses that turn a read statement into a write: `WITH ... INSERT`,
# `SELECT ... INTO`, `SELECT ... FOR UPDATE`.
_FORBIDDEN_CLAUSE_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE", "INTO"})

_ASCII_WHITESPACE = frozenset(" \t\n\r\v\f")

# Connector/Python (`mysql.connector._scripting.DELIMITER_PATTERN`) treats a
# match as a client-side DELIMITER command and rewrites the SQL before sending
# it, so the server would not run the text the gate checked.
_CONNECTOR_DELIMITER = re.compile(
    rb"""(delimiter\s+)(?=(?:[^"'`]*(?:"[^"]*"|'[^']*'|`[^`]*`))*[^"'`]*$)""",
    flags=re.IGNORECASE | re.MULTILINE,
)

# Prefixes the MySQL dialect reads as part of a string literal when they start
# a token, e.g. B'..', which takes no backslash escapes.
_STRING_PREFIXES = frozenset("bBnNxX")


class SqlLexError(ValueError):
    """SQL whose strings or comments cannot be delimited the way the server does."""


def _is_word_char(ch: str) -> bool:
    return ch.isalnum() or ch in "_$"


def _quoted_end(sql: str, start: int, backslash_escape: bool) -> int:
    quote = sql[start]
    i = start + 1
    while i < len(sql):
        ch = sql[i]
        if backslash_escape and ch == "\\":
            i += 2
        elif ch == quote:
            if sql.startswith(quote, i + 1):
                i += 2
            else:
                return i + 1
        else:
            i += 1
    raise SqlLexError("Unterminated quoted string or identifier")


def _line_end(sql: str, start: int) -> int:
    # The MySQL dialect ends line comments at `\n` only, not `\r`.
    end = sql.find("\n", start)
    return len(sql) if end < 0 else end + 1


def _has_string_prefix(sql: str, quote: int) -> bool:
    if quote == 0 or sql[quote - 1] not in _STRING_PREFIXES:
        return False
    # Inside a longer word (`SELECT'a'`) the quote opens a plain string. After
    # a digit the server's reading depends on numeric-prefix rules; reject.
    before = sql[quote - 2] if quote >= 2 else ""
    return not (before.isalpha() or before in ("_", "$"))


def _lex(sql: str) -> list[tuple[str, str]]:
    """Split SQL into (kind, text) tokens, kind in word/punct/literal/comment.

    Mirrors the MySQL dialect tokenizer GreptimeDB applies to the MySQL
    protocol (sqlparser `MySqlDialect`). The gate is only as sound as this
    agreement: a string or comment boundary that differs from the server's
    hides code from the gate. Where the server's reading is not certain, the
    input is rejected.
    """
    tokens = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
        elif ch in "'\"":
            if _has_string_prefix(sql, i):
                raise SqlLexError("Prefixed string literals are not allowed")
            end = _quoted_end(sql, i, backslash_escape=True)
            tokens.append(("literal", sql[i:end]))
            i = end
        elif ch == "`":
            end = _quoted_end(sql, i, backslash_escape=False)
            tokens.append(("literal", sql[i:end]))
            i = end
        elif ch == "#":
            end = _line_end(sql, i)
            tokens.append(("comment", sql[i:end]))
            i = end
        elif sql.startswith("--", i) and i + 2 < n and sql[i + 2] in _ASCII_WHITESPACE:
            end = _line_end(sql, i)
            tokens.append(("comment", sql[i:end]))
            i = end
        elif sql.startswith("--", i) and i + 2 < n and not sql[i + 2].isascii():
            # `--` is a comment only before whitespace, and the server's notion
            # of non-ASCII whitespace is not Python's.
            raise SqlLexError("Ambiguous `--` sequence")
        elif sql.startswith("/*", i):
            if sql.startswith("!", i + 2):
                # The MySQL dialect parses the body of `/*! ... */` as SQL.
                raise SqlLexError("MySQL executable comments are not allowed")
            end = sql.find("*/", i + 2)
            if end < 0:
                raise SqlLexError("Unterminated comment")
            tokens.append(("comment", sql[i : end + 2]))
            i = end + 2
        elif _is_word_char(ch):
            end = i + 1
            while end < n and _is_word_char(sql[end]):
                end += 1
            tokens.append(("word", sql[i:end].upper()))
            i = end
        else:
            tokens.append(("punct", ch))
            i += 1
    return tokens


def _split_statements(tokens: list[tuple[str, str]]) -> list[list[tuple[str, str]]]:
    statements = [[]]
    for token in tokens:
        if token[0] == "comment":
            continue
        if token == ("punct", ";"):
            statements.append([])
        else:
            statements[-1].append(token)
    return [stmt for stmt in statements if stmt]


def _statement_violation(stmt: list[tuple[str, str]]) -> str | None:
    i = 0
    while i < len(stmt) and stmt[i] == ("punct", "("):
        i += 1
    if i == len(stmt) or stmt[i][0] != "word":
        return "Unrecognized statement"

    keyword = stmt[i][1]
    if keyword not in READ_STATEMENT_KEYWORDS:
        return f"Forbidden `{keyword}` operation"

    if keyword == "EXPLAIN":
        # EXPLAIN [ANALYZE] [VERBOSE] [FORMAT <format>] <statement>
        i += 1
        for modifier in ("ANALYZE", "VERBOSE"):
            if i < len(stmt) and stmt[i] == ("word", modifier):
                i += 1
        if i < len(stmt) and stmt[i] == ("word", "FORMAT"):
            i += 2
        return _statement_violation(stmt[i:])

    if keyword == "TQL":
        # The server hands the rest of the statement to the PromQL parser, so
        # its words are metric and label names, not SQL clauses.
        return None

    for kind, text in _skip_tql_ctes(stmt[i + 1 :]):
        if kind == "word" and text in _FORBIDDEN_CLAUSE_KEYWORDS:
            return f"Forbidden `{text}` operation"
    return None


def _skip_tql_ctes(tokens: list[tuple[str, str]]):
    """Yield tokens outside `(TQL ...)` groups.

    In `WITH m AS (TQL ...)` the server hands everything up to the matching `)`
    to the PromQL parser. Elsewhere a `(TQL` group fails to parse as SQL.
    """
    i = 0
    while i < len(tokens):
        if tokens[i] == ("punct", "(") and tokens[i + 1 : i + 2] == [("word", "TQL")]:
            depth = 0
            while i < len(tokens):
                if tokens[i] == ("punct", "("):
                    depth += 1
                elif tokens[i] == ("punct", ")"):
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
        else:
            yield tokens[i]
        i += 1


def statement_keywords(query: str) -> tuple[str, ...]:
    """Words of the first statement, comments skipped; empty if it does not lex."""
    try:
        statements = _split_statements(_lex(query))
    except SqlLexError:
        return ()
    if not statements:
        return ()
    return tuple(text for kind, text in statements[0] if kind == "word")


def security_gate(query: str) -> tuple[bool, str]:
    """Check that SQL consists only of read statements.

    Returns (is_dangerous, reason). Every statement must start with a keyword
    from READ_STATEMENT_KEYWORDS and must not carry a write clause.
    """
    if not query or not query.strip():
        return True, "Empty query not allowed"

    if _CONNECTOR_DELIMITER.search(query.encode()):
        return True, "`delimiter` outside quotes is rewritten by the client driver"

    try:
        statements = _split_statements(_lex(query))
    except SqlLexError as e:
        logger.warning(f"Unparsable query rejected: {query[:50]}...")
        return True, str(e)

    if not statements:
        return True, "Empty query not allowed"

    for stmt in statements:
        reason = _statement_violation(stmt)
        if reason:
            logger.warning(f"Dangerous pattern detected: {query[:50]}...")
            return True, reason

    return False, ""


def templates_loader() -> dict[str, dict[str, Any]]:
    templates = {}
    template_dir = os.path.join(os.path.dirname(__file__), "templates")

    for category in os.listdir(template_dir):
        category_path = os.path.join(template_dir, category)
        if os.path.isdir(category_path):
            config_path = os.path.join(category_path, "config.yaml")
            template_path = os.path.join(category_path, "template.md")
            if not os.path.exists(config_path) or not os.path.exists(template_path):
                logger.warning("Skipping incomplete prompt template: %s", category)
                continue

            # Load config
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Load template
            with open(template_path, "r", encoding="utf-8") as f:
                template = f.read()

            templates[category] = {"config": config, "template": template}

    return templates


@lru_cache(maxsize=128)
def _compile_prompt_template(template: str) -> Template:
    """Compile and cache a prompt template. Templates are static strings."""
    return _PROMPT_TEMPLATE_ENV.from_string(template)


def render_prompt_template(template: str, context: dict[str, Any]) -> str:
    """Render a prompt template with a restricted Jinja environment."""
    try:
        return _compile_prompt_template(template).render(**context)
    except TemplateError as e:
        raise ValueError(f"Failed to render prompt template: {e}") from e


# Validation patterns
TABLE_NAME_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){0,2}$"
)
DURATION_PATTERN = re.compile(r"^(\d+)(ms|s|m|h|d|w|y)$")
FILL_PATTERN = re.compile(r"^(NULL|PREV|LINEAR|(-?\d+(\.\d+)?))$", re.IGNORECASE)


def validate_table_name(table: str) -> str:
    """Validate table name format.

    Accepts unquoted GreptimeDB-style names with up to three segments:
    'table', 'schema.table', or 'catalog.schema.table'. Quoted identifiers
    (backticks) are not supported.
    """
    if not table:
        raise ValueError("Table name is required")
    if not TABLE_NAME_PATTERN.match(table):
        raise ValueError(
            "Invalid table name: must be 'table', 'schema.table', or "
            "'catalog.schema.table' format (unquoted identifiers only)"
        )
    return table


def validate_tql_param(value: str, name: str) -> str:
    """Validate TQL parameter doesn't contain injection characters."""
    if not value:
        raise ValueError(f"{name} is required")
    if "'" in value or ";" in value or "--" in value:
        raise ValueError(f"Invalid characters in {name}")
    return value


def validate_query_component(value: str, name: str) -> str:
    """Reject a fragment that could change the shape of the statement around it.

    The assembled statement still goes through security_gate; this keeps a
    fragment from ending the statement or commenting out the clauses after it.
    """
    if not value:
        return value
    try:
        tokens = _lex(value)
    except SqlLexError as e:
        raise ValueError(f"Dangerous pattern in {name}: {e}") from e

    depth = 0
    for kind, text in tokens:
        reason = None
        if kind == "comment":
            reason = "Comments are not allowed"
        elif kind == "punct" and text == ";":
            reason = "Multiple statements are not allowed"
        elif kind == "punct" and text == "(":
            depth += 1
        elif kind == "punct" and text == ")":
            depth -= 1
            if depth < 0:
                reason = "Unbalanced parentheses"
        if reason:
            raise ValueError(f"Dangerous pattern in {name}: {reason}")
    if depth != 0:
        raise ValueError(f"Dangerous pattern in {name}: Unbalanced parentheses")
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
    if is_sql_time_expression(value):
        validate_query_component(value, name)
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
