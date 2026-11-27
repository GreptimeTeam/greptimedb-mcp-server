import pytest
import datetime
import json
from greptimedb_mcp_server.utils import (
    templates_loader,
    render_prompt_template,
    security_gate,
    validate_table_name,
    is_sql_time_expression,
    format_tql_time_param,
    validate_time_expression,
    validate_query_component,
    _truncate_value,
    _format_audit_params,
    audit_log,
)
from greptimedb_mcp_server.formatter import format_results, truncate_to_budget


def test_templates_loader_basic():
    """Test that templates_loader can load existing templates"""
    # Call the function under test
    templates = templates_loader()

    # Basic validation that we got something back
    assert templates is not None

    # Check if templates is a dictionary
    assert isinstance(templates, dict), "Expected templates to be a dictionary"
    assert len(templates) > 0, "Expected templates dictionary to have items"

    # Check if the metrics_analysis template is in the dictionary
    assert "metrics_analysis" in templates, "metrics_analysis template not found"

    # Get the metrics_analysis template
    metrics_template = templates["metrics_analysis"]

    # Get the metrics_analysis template config
    config = metrics_template["config"]

    # Check that the config has the expected structure
    assert isinstance(config, dict), "Expected template to be a dictionary"
    assert "description" in config, "Template config missing 'description' field"
    assert "arguments" in config, "Template config missing 'arguments' field"
    assert "metadata" in config, "Template config missing 'metadata' field"

    # Check that the template has the expected arguments
    arguments = config["arguments"]
    assert isinstance(arguments, list), "Expected arguments to be a list"

    arg_names = [
        arg.get("name") for arg in arguments if isinstance(arg, dict) and "name" in arg
    ]
    expected_args = ["topic", "start_time", "end_time"]
    for arg in expected_args:
        assert (
            arg in arg_names
        ), f"Expected argument '{arg}' not found in metrics_analysis template"

    # Check template content
    tpl = metrics_template["template"]
    assert "{{ topic }}" in tpl
    assert "{{ start_time }}" in tpl
    assert "{{ end_time }}" in tpl


def test_template_variable_rendering():
    """Test that template variables are correctly rendered."""
    templates = templates_loader()

    template = templates["pipeline_creator"]["template"]
    result = render_prompt_template(
        template,
        {"log_sample": "test log line", "pipeline_name": "my_test_pipeline"},
    )

    assert "{{ log_sample }}" not in result, "log_sample variable was not replaced"
    assert (
        "{{ pipeline_name }}" not in result
    ), "pipeline_name variable was not replaced"
    assert "test log line" in result, "log_sample value not found in result"
    assert "my_test_pipeline" in result, "pipeline_name value not found in result"


def test_template_supports_jinja_conditionals_and_defaults():
    """Prompt templates support a restricted subset of Jinja."""
    template = """{% if search_term %}
Search: {{ search_term }}
{% else %}
Search: {{ search_term | default("error", true) }}
{% endif %}"""

    assert "Search: error" in render_prompt_template(template, {"search_term": ""})
    assert "Search: timeout" in render_prompt_template(
        template, {"search_term": "timeout"}
    )


def test_optional_prompt_arguments_default_to_empty_string():
    """Prompt functions should allow optional arguments to be omitted."""
    result = render_prompt_template(
        templates_loader()["log_pipeline"]["template"],
        {"table": "logs", "search_term": ""},
    )

    assert "{{ table }}" not in result
    assert "{{ search_term }}" not in result
    assert "Log Analysis: logs" in result


def test_all_prompt_templates_render_with_configured_arguments():
    """Every prompt template should render with the arguments declared in config."""
    expected_prompts = {
        "ingestion_troubleshooting",
        "log_pipeline",
        "metrics_analysis",
        "observability_correlation",
        "pipeline_creator",
        "promql_analysis",
        "query_performance_tuning",
        "schema_design_advisor",
        "table_operation",
        "trace_analysis",
    }
    templates = templates_loader()

    assert set(templates.keys()) == expected_prompts

    for name, template_data in templates.items():
        context = {}
        for arg in template_data["config"].get("arguments", []):
            if not isinstance(arg, dict) or "name" not in arg:
                continue
            context[arg["name"]] = (
                f"sample_{arg['name']}" if arg.get("required") else ""
            )

        rendered = render_prompt_template(template_data["template"], context)

        assert "{{" not in rendered, f"{name} left an unrendered expression"
        assert "{%" not in rendered, f"{name} left an unrendered block"


def test_template_undefined_variable_is_rejected():
    """Misspelled prompt variables should not render silently."""
    with pytest.raises(ValueError, match="missing_value"):
        render_prompt_template("{{ missing_value }}", {})


def test_template_sandbox_blocks_unsafe_attribute_access():
    """Templates should not be able to access Python internals."""
    with pytest.raises(ValueError):
        render_prompt_template("{{ value.__class__.__mro__ }}", {"value": "x"})


def test_empty_queries():
    """Test empty queries"""
    assert security_gate("") == (True, "Empty query not allowed")
    assert security_gate("   ") == (True, "Empty query not allowed")
    assert security_gate(None) == (True, "Empty query not allowed")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM users",
        "SELECT * FROM users; SELECT * FROM test",
        "SELECT 1;",
        "(SELECT 1)",
        "EXPLAIN SELECT * FROM users",
        "EXPLAIN ANALYZE VERBOSE SELECT * FROM users",
        "WITH t AS (SELECT 1) SELECT * FROM t",
        "TQL EVAL ('now-1h', 'now', '1m') rate(x[5m])",
        "SHOW TABLES",
        "SHOW CREATE TABLE my_table",
        "show create table my_schema.my_table",
        "DESCRIBE users",
        "SELECT * FROM users UNION SELECT * FROM admins",
        "SELECT * FROM INFORMATION_SCHEMA.TABLES",
        # Encoded values cannot become statements without dynamic SQL, which
        # is not an allowed leading keyword.
        "SELECT 0x44524f50205441424c45",
        "SELECT UNHEX('44524f50')",
        "SELECT CHAR(68,82,79,80)",
        # Write keywords inside strings, quoted identifiers and comments.
        "SELECT 'DROP TABLE users; --' AS s",
        'SELECT `delete`, "update" FROM t',
        "/* DROP TABLE users */ SELECT 1",
        "-- DROP TABLE users\nSELECT 1",
        "# DROP TABLE users\nSELECT 1",
        # `--` without trailing whitespace is two minus signs.
        "SELECT 1--1",
        # The backslash escapes the quote, so this is one string literal.
        "SELECT 'a\\'; DROP TABLE t; -- '",
        "SELECT 'it''s'",
        # Line comments end at \n only, so the DROP is part of the comment.
        "SELECT 1 -- x\r; DROP TABLE t",
        # A quote right after a keyword opens a plain string.
        "SELECT'ok'",
        "SELECT 1 WHERE 'abc' LIKE'a%'",
        # PromQL label names are not SQL clauses.
        "TQL EVAL (0, 10, '5s') sum by (merge) (requests_total)",
        "WITH m AS (TQL EVAL (0, 10, '5s') sum by (merge) (requests_total)) "
        "SELECT * FROM m",
    ],
)
def test_security_gate_allows_reads(query):
    assert security_gate(query) == (False, "")


@pytest.mark.parametrize(
    "query,reason",
    [
        ("DROP TABLE users", "Forbidden `DROP` operation"),
        ("DELETE FROM users", "Forbidden `DELETE` operation"),
        ("UPDATE users SET name='test'", "Forbidden `UPDATE` operation"),
        ("INSERT INTO users VALUES (1)", "Forbidden `INSERT` operation"),
        ("ALTER TABLE users ADD COLUMN x INT", "Forbidden `ALTER` operation"),
        ("CREATE TABLE new_table (id INT)", "Forbidden `CREATE` operation"),
        ("GRANT SELECT ON users TO user1", "Forbidden `GRANT` operation"),
        ("REVOKE SELECT ON users FROM user1", "Forbidden `REVOKE` operation"),
        ("TRUNCATE TABLE users", "Forbidden `TRUNCATE` operation"),
        ("LOAD DATA INFILE '/etc/passwd' INTO TABLE users", "`LOAD`"),
        ("COPY users TO '/tmp/data.csv'", "Forbidden `COPY` operation"),
        ("EXEC sp_executesql 'SELECT 1'", "Forbidden `EXEC` operation"),
        ("EXECUTE immediate 'SELECT * FROM users'", "Forbidden `EXECUTE` operation"),
        ("CALL stored_procedure()", "Forbidden `CALL` operation"),
        ("REPLACE INTO users VALUES (1, 'test')", "Forbidden `REPLACE` operation"),
        ("ADMIN flush_table('t')", "Forbidden `ADMIN` operation"),
        ("SET time_zone = 'UTC'", "Forbidden `SET` operation"),
        ("DROP/**/TABLE users", "Forbidden `DROP` operation"),
        ("DROP--comment\nTABLE users", "Forbidden `DROP` operation"),
        ("SELECT * FROM users; DROP TABLE test", "Forbidden `DROP` operation"),
        ("SELECT * FROM users INTO OUTFILE '/tmp/u.txt'", "Forbidden `INTO` operation"),
        ("SELECT 'test' INTO DUMPFILE '/tmp/test.txt'", "Forbidden `INTO` operation"),
        ("SELECT * FROM t FOR UPDATE", "Forbidden `UPDATE` operation"),
        ("WITH t AS (SELECT 1) INSERT INTO x SELECT * FROM t", "`INSERT`"),
        ("EXPLAIN ANALYZE INSERT INTO t VALUES (1)", "Forbidden `INSERT` operation"),
        ("EXPLAIN ANALYZE FORMAT JSON DROP TABLE t", "Forbidden `DROP` operation"),
        (
            "WITH m AS (TQL EVAL (0, 10, '5s') up) SELECT * FROM m INTO OUTFILE 'f'",
            "Forbidden `INTO` operation",
        ),
        # A comment opener inside a line comment or a string does not start a
        # block comment, so the DROP after it is code.
        ("-- x /*\nDROP TABLE users /* */", "Forbidden `DROP` operation"),
        ("# x /*\nDROP TABLE users /* */", "Forbidden `DROP` operation"),
        ("SELECT '/*'; DROP TABLE t -- */", "Forbidden `DROP` operation"),
        ("/*!DROP TABLE t*/ SELECT 1", "MySQL executable comments are not allowed"),
        (
            "SELECT X'00'; DROP TABLE t; -- '",
            "Prefixed string literals are not allowed",
        ),
        ("SELECT 1b'\\'; DROP TABLE t; -- '", "Prefixed string literals"),
        # Connector/Python rewrites these before sending them.
        ("SELECT 'ok' AS field_delimiter FROM numbers", "`delimiter`"),
        (
            "SELECT 1 AS x_delimiter ' FROM t; INSERT INTO p VALUES (1); -- '",
            "`delimiter`",
        ),
        ("SELECT 1 --\u00a0x", "Ambiguous `--` sequence"),
        ("SELECT 'abc", "Unterminated quoted string or identifier"),
        ("SELECT `abc", "Unterminated quoted string or identifier"),
        ("SELECT 1 /* x", "Unterminated comment"),
        ("/* only a comment */", "Empty query not allowed"),
        ("'abc'", "Unrecognized statement"),
    ],
)
def test_security_gate_blocks(query, reason):
    is_dangerous, actual = security_gate(query)
    assert is_dangerous is True
    assert reason in actual


def test_format_results_csv():
    """Test format_results with CSV format"""
    result = format_results(["a", "b"], [(1, 2), (3, 4)], "csv")
    assert "a,b" in result
    assert "1,2" in result
    assert "3,4" in result


def test_format_results_json():
    """Test format_results with JSON format"""
    result = format_results(["a", "b"], [(1, 2)], "json")
    data = json.loads(result)
    assert len(data) == 1
    assert data[0]["a"] == 1
    assert data[0]["b"] == 2


def test_format_results_json_non_native_values():
    """Test JSON format handles non-JSON-native values without crashing."""
    from decimal import Decimal

    result = format_results(
        ["amount", "payload"],
        [(Decimal("3.14"), b"\x00\x01")],
        "json",
        mask_enabled=False,
    )
    data = json.loads(result)
    assert data[0]["amount"] == "3.14"
    assert data[0]["payload"] == "b'\\x00\\x01'"


def test_format_results_markdown():
    """Test format_results with markdown format"""
    result = format_results(["a", "b"], [(1, 2)], "markdown")
    assert "| a | b |" in result
    assert "| --- | --- |" in result
    assert "| 1 | 2 |" in result


def test_format_results_empty_rows():
    """Test format_results with empty rows"""
    result = format_results(["a", "b"], [], "markdown")
    assert "| a | b |" in result
    assert "| --- | --- |" in result


def test_format_results_json_with_datetime():
    """Test format_results JSON format with datetime values"""
    dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
    result = format_results(["ts", "value"], [(dt, 100)], "json")
    data = json.loads(result)
    assert data[0]["ts"] == "2024-01-01 12:00:00"
    assert data[0]["value"] == 100


def test_format_results_markdown_with_none():
    """Test format_results markdown format with None values"""
    result = format_results(["a", "b"], [(1, None)], "markdown")
    assert "| 1 |  |" in result


def test_format_results_default_csv():
    """Test format_results defaults to CSV when format not specified"""
    result = format_results(["a"], [(1,)])
    assert "a" in result
    assert "1" in result


def test_format_results_csv_quotes_special_chars():
    """Test format_results CSV properly quotes values with special characters"""
    result = format_results(
        ["name", "desc"], [("hello", "has,comma"), ("world", 'has"quote')], "csv"
    )
    assert "name,desc" in result
    assert "hello" in result
    assert '"has,comma"' in result  # comma values get quoted
    assert '"has""quote"' in result  # quotes get escaped and value quoted


def test_format_results_markdown_escapes_pipe():
    """Test format_results markdown format escapes pipe characters"""
    result = format_results(["col|name", "value"], [("a|b", "c|d")], "markdown")
    assert r"col\|name" in result
    assert r"a\|b" in result
    assert r"c\|d" in result


@pytest.mark.parametrize(
    "value",
    ["avg(cpu) RANGE '5m'", "host = 'a;b' AND (x > 1)", "count(*) RANGE '1m' FILL 0"],
)
def test_validate_query_component_allows(value):
    assert validate_query_component(value, "select") == value


@pytest.mark.parametrize(
    "value,reason",
    [
        ("1=1; DROP TABLE users", "Multiple statements are not allowed"),
        ("1=1 -- rest", "Comments are not allowed"),
        ("1=1 /* rest */", "Comments are not allowed"),
        ("x) UNION (SELECT 1", "Unbalanced parentheses"),
        ("(x", "Unbalanced parentheses"),
        ("host = 'a", "Unterminated quoted string or identifier"),
    ],
)
def test_validate_query_component_blocks(value, reason):
    with pytest.raises(ValueError) as excinfo:
        validate_query_component(value, "where")
    assert reason in str(excinfo.value)


def test_validate_table_name_simple():
    """Test validate_table_name with simple table names"""
    assert validate_table_name("users") == "users"
    assert validate_table_name("my_table") == "my_table"
    assert validate_table_name("Table123") == "Table123"


def test_validate_table_name_schema_qualified():
    """Test validate_table_name with schema.table format"""
    assert validate_table_name("public.users") == "public.users"
    assert validate_table_name("schema_name.table_name") == "schema_name.table_name"
    assert validate_table_name("my_schema.my_table") == "my_schema.my_table"


def test_validate_table_name_catalog_qualified():
    """Test validate_table_name accepts GreptimeDB-style catalog.schema.table"""
    assert validate_table_name("greptime.public.users") == "greptime.public.users"
    assert validate_table_name("cat.schema_name.tbl") == "cat.schema_name.tbl"


def test_validate_table_name_invalid():
    """Test validate_table_name rejects invalid names"""
    with pytest.raises(ValueError) as excinfo:
        validate_table_name("123invalid")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("table-with-dash")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("catalog.schema.table.extra")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("")
    assert "required" in str(excinfo.value)


# Tests for time expression functions


def test_is_sql_time_expression_with_function():
    """Test is_sql_time_expression detects SQL function calls"""
    assert is_sql_time_expression("now()") is True
    assert is_sql_time_expression("now() - interval '5' minute") is True
    assert is_sql_time_expression("date_trunc('day', now())") is True
    assert is_sql_time_expression("NOW()") is True


def test_is_sql_time_expression_without_function():
    """Test is_sql_time_expression returns False for non-SQL expressions"""
    assert is_sql_time_expression("2024-01-01T00:00:00Z") is False
    assert is_sql_time_expression("1704067200") is False
    assert is_sql_time_expression("1704067200.123") is False


def test_format_tql_time_param_sql_expression():
    """Test format_tql_time_param leaves SQL expressions unquoted"""
    assert format_tql_time_param("now()") == "now()"
    assert format_tql_time_param("now() - interval '5' minute") == (
        "now() - interval '5' minute"
    )
    assert format_tql_time_param("date_trunc('day', now())") == (
        "date_trunc('day', now())"
    )


def test_format_tql_time_param_literal():
    """Test format_tql_time_param quotes literal values"""
    assert format_tql_time_param("2024-01-01T00:00:00Z") == "'2024-01-01T00:00:00Z'"
    assert format_tql_time_param("1704067200") == "'1704067200'"
    assert format_tql_time_param("1704067200.123") == "'1704067200.123'"


def test_validate_time_expression_valid():
    """Test validate_time_expression accepts valid expressions"""
    assert validate_time_expression("now()", "start") == "now()"
    assert validate_time_expression("now() - interval '5' minute", "start") == (
        "now() - interval '5' minute"
    )
    assert validate_time_expression("2024-01-01T00:00:00Z", "end") == (
        "2024-01-01T00:00:00Z"
    )
    assert validate_time_expression("1704067200", "start") == "1704067200"


def test_validate_time_expression_empty():
    """Test validate_time_expression rejects empty values"""
    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("", "start")
    assert "start is required" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("", "end")
    assert "end is required" in str(excinfo.value)


def test_validate_time_expression_injection():
    """Test validate_time_expression blocks injection attempts"""
    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("now(); DROP TABLE users", "start")
    assert "Invalid characters" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("now() -- comment", "end")
    assert "Invalid characters" in str(excinfo.value)


def test_validate_time_expression_unbalanced_quotes():
    """Test validate_time_expression blocks unbalanced quotes"""
    # Odd number of quotes should be rejected
    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("2024-01-01T00:00:00Z' OR 1=1", "start")
    assert "Unbalanced quotes" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_time_expression("now() - interval '5 minute", "end")
    assert "Unbalanced quotes" in str(excinfo.value)

    # Balanced quotes are allowed
    result = validate_time_expression("now() - interval '5' minute", "start")
    assert result == "now() - interval '5' minute"


def test_format_tql_time_param_escapes_quotes():
    """Test format_tql_time_param escapes quotes in literals"""
    # Quotes in literals should be escaped
    assert format_tql_time_param("test'value") == "'test''value'"
    assert format_tql_time_param("a''b") == "'a''''b'"
    # SQL expressions are not escaped
    assert format_tql_time_param("now()") == "now()"


# Tests for audit logging functions


def test_truncate_value():
    """Test _truncate_value truncates long values"""
    assert _truncate_value("short") == "short"
    assert _truncate_value("a" * 201) == "a" * 200 + "..."


def test_format_audit_params():
    """Test _format_audit_params formats params correctly"""
    assert _format_audit_params({}) == ""
    assert _format_audit_params({"query": "SELECT 1"}) == 'query="SELECT 1"'


def test_audit_log(caplog):
    """Test audit_log records tool calls"""
    import logging

    with caplog.at_level(logging.INFO, logger="greptimedb_mcp_server.audit"):
        audit_log("execute_sql", {"query": "SELECT 1"}, success=True, duration_ms=10.5)

    assert len(caplog.records) == 1
    msg = caplog.records[0].message
    assert "[AUDIT] execute_sql" in msg
    assert "success=True" in msg


def test_audit_log_never_raises():
    """Test audit_log never raises exceptions"""
    audit_log(None, None, None, None, None)  # Should not raise


def test_truncate_to_budget_leaves_a_fitting_result_alone():
    text = "small enough"
    assert truncate_to_budget(text, 1024, "csv") == text


def test_truncate_to_budget_holds_the_budget_and_says_it_cut():
    result = truncate_to_budget("x" * 5000, 500, "json")

    assert len(result.encode("utf-8")) <= 500
    assert "truncated" in result
    assert "json" in result


def test_truncate_to_budget_does_not_split_a_character():
    """A cut landing inside a multi-byte character must not corrupt it."""
    result = truncate_to_budget("中" * 2000, 600, "csv")

    assert len(result.encode("utf-8")) <= 600
    result.encode("utf-8").decode("utf-8")  # raises if a character was split


def test_budget_backstop_does_not_invent_tool_parameters():
    result = truncate_to_budget("x" * 1000, 300, "json")
    assert "truncated" in result
    assert "`limit`" not in result
    assert "select fewer columns" not in result
