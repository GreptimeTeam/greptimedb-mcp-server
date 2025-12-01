import pytest
import datetime
import json
from greptimedb_mcp_server.utils import (
    templates_loader,
    security_gate,
    validate_table_name,
)
from greptimedb_mcp_server.formatter import format_results


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
    """Test that template variables {{ key }} are correctly rendered."""
    from typing import Annotated

    templates = templates_loader()

    # Test with pipeline_creator template which has variables
    if "pipeline_creator" in templates:
        template_data = templates["pipeline_creator"]
        config = template_data["config"]
        template_content = template_data["template"]

        args_config = config.get("arguments", [])
        arg_info = [
            (arg["name"], arg.get("description", ""), arg.get("required", False))
            for arg in args_config
            if isinstance(arg, dict) and "name" in arg
        ]

        # Build the prompt function dynamically (same logic as server.py)
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

        # Test rendering with sample values
        result = prompt_fn(log_sample="test log line", pipeline_name="my_test_pipeline")

        # Verify variables were replaced
        assert "{{ log_sample }}" not in result, "log_sample variable was not replaced"
        assert (
            "{{ pipeline_name }}" not in result
        ), "pipeline_name variable was not replaced"
        assert "test log line" in result, "log_sample value not found in result"
        assert "my_test_pipeline" in result, "pipeline_name value not found in result"


def test_empty_queries():
    """Test empty queries"""
    assert security_gate("") == (True, "Empty query not allowed")
    assert security_gate("   ") == (True, "Empty query not allowed")
    assert security_gate(None) == (True, "Empty query not allowed")


def test_dangerous_operations():
    """Test dangerous operations"""
    assert security_gate("DROP TABLE users") == (True, "Forbidden `DROP` operation")
    assert security_gate("DELETE FROM users") == (True, "Forbidden `DELETE` operation")
    assert security_gate("UPDATE users SET name='test'") == (
        True,
        "Forbidden `UPDATE` operation",
    )
    assert security_gate("INSERT INTO users VALUES (1)") == (
        True,
        "Forbidden `INSERT` operation",
    )


def test_multiple_statements():
    """Test multiple statements with dangerous operations"""
    # Safe multiple SELECT statements are now allowed
    assert security_gate("SELECT * FROM users; SELECT * FROM test") == (False, "")
    # Dangerous multiple statements are blocked (DROP detected first)
    result = security_gate("SELECT * FROM users; DROP TABLE test")
    assert result[0] is True


def test_comment_bypass():
    """Test comment bypass attempts"""
    assert security_gate("DROP/**/TABLE users") == (True, "Forbidden `DROP` operation")
    assert security_gate("DROP--comment\nTABLE users") == (
        True,
        "Forbidden `DROP` operation",
    )


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT * FROM users", (False, "")),
        ("DROP TABLE users", (True, "Forbidden `DROP` operation")),
        ("DELETE FROM users", (True, "Forbidden `DELETE` operation")),
        ("", (True, "Empty query not allowed")),
    ],
)
def test_parametrized_queries(query, expected):
    """Parametrized test for multiple queries"""
    assert security_gate(query) == expected


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


def test_security_gate_alter():
    """Test security gate blocks ALTER operations"""
    assert security_gate("ALTER TABLE users ADD COLUMN x INT")[0] is True


def test_security_gate_create():
    """Test security gate blocks CREATE operations"""
    assert security_gate("CREATE TABLE new_table (id INT)")[0] is True


def test_security_gate_grant():
    """Test security gate blocks GRANT operations"""
    assert security_gate("GRANT SELECT ON users TO user1")[0] is True


def test_security_gate_revoke():
    """Test security gate blocks REVOKE operations"""
    assert security_gate("REVOKE SELECT ON users FROM user1")[0] is True


def test_security_gate_truncate():
    """Test security gate blocks TRUNCATE operations"""
    assert security_gate("TRUNCATE TABLE users")[0] is True


def test_security_gate_explain():
    """Test security gate allows EXPLAIN operations"""
    result = security_gate("EXPLAIN SELECT * FROM users")
    assert result[0] is False


def test_security_gate_with_cte():
    """Test security gate allows CTE (WITH) queries"""
    result = security_gate("WITH t AS (SELECT 1) SELECT * FROM t")
    assert result[0] is False


def test_security_gate_tql():
    """Test security gate allows TQL queries"""
    result = security_gate("TQL EVAL ('now-1h', 'now', '1m') rate(x[5m])")
    assert result[0] is False


def test_security_gate_show():
    """Test security gate allows SHOW queries"""
    result = security_gate("SHOW TABLES")
    assert result[0] is False


def test_security_gate_show_create_table():
    """Test security gate allows SHOW CREATE TABLE queries"""
    result = security_gate("SHOW CREATE TABLE my_table")
    assert result[0] is False
    result = security_gate("show create table my_schema.my_table")
    assert result[0] is False


def test_security_gate_desc():
    """Test security gate allows DESC/DESCRIBE queries"""
    result = security_gate("DESCRIBE users")
    assert result[0] is False


def test_security_gate_load():
    """Test security gate blocks LOAD operations"""
    result = security_gate("LOAD DATA INFILE '/etc/passwd' INTO TABLE users")
    assert result[0] is True
    assert "LOAD" in result[1]


def test_security_gate_copy():
    """Test security gate blocks COPY operations"""
    result = security_gate("COPY users TO '/tmp/data.csv'")
    assert result[0] is True
    assert "COPY" in result[1]


def test_security_gate_outfile():
    """Test security gate blocks OUTFILE operations"""
    result = security_gate("SELECT * FROM users INTO OUTFILE '/tmp/users.txt'")
    assert result[0] is True
    assert "OUTFILE" in result[1]


def test_security_gate_load_file():
    """Test security gate blocks LOAD_FILE function"""
    result = security_gate("SELECT LOAD_FILE('/etc/passwd')")
    assert result[0] is True
    assert "LOAD_FILE" in result[1]


def test_security_gate_union():
    """Test security gate allows UNION operations"""
    result = security_gate("SELECT * FROM users UNION SELECT * FROM admins")
    assert result[0] is False


def test_security_gate_information_schema():
    """Test security gate allows INFORMATION_SCHEMA access"""
    result = security_gate("SELECT * FROM INFORMATION_SCHEMA.TABLES")
    assert result[0] is False


def test_security_gate_dumpfile():
    """Test security gate blocks INTO DUMPFILE operations"""
    result = security_gate("SELECT 'test' INTO DUMPFILE '/tmp/test.txt'")
    assert result[0] is True
    assert "DUMPFILE" in result[1]


def test_security_gate_exec():
    """Test security gate blocks EXEC/EXECUTE operations"""
    result = security_gate("EXEC sp_executesql 'SELECT 1'")
    assert result[0] is True
    assert "Dynamic SQL" in result[1]

    result = security_gate("EXECUTE immediate 'SELECT * FROM users'")
    assert result[0] is True
    assert "Dynamic SQL" in result[1]


def test_security_gate_call():
    """Test security gate blocks CALL operations"""
    result = security_gate("CALL stored_procedure()")
    assert result[0] is True
    assert "Stored procedure" in result[1]


def test_security_gate_replace_into():
    """Test security gate blocks REPLACE INTO operations"""
    result = security_gate("REPLACE INTO users VALUES (1, 'test')")
    assert result[0] is True
    assert "REPLACE INTO" in result[1]


def test_security_gate_hex_encoding():
    """Test security gate blocks hex-encoded content"""
    result = security_gate("SELECT 0x44524f50205441424c45")
    assert result[0] is True
    assert "Encoded" in result[1]


def test_security_gate_unhex():
    """Test security gate blocks UNHEX function"""
    result = security_gate("SELECT UNHEX('44524f50')")
    assert result[0] is True
    assert "Encoded" in result[1]


def test_security_gate_char_function():
    """Test security gate blocks CHAR function for encoding bypass"""
    result = security_gate("SELECT CHAR(68,82,79,80)")
    assert result[0] is True
    assert "Encoded" in result[1]


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


def test_validate_table_name_invalid():
    """Test validate_table_name rejects invalid names"""
    with pytest.raises(ValueError) as excinfo:
        validate_table_name("123invalid")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("table-with-dash")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("schema.table.extra")
    assert "Invalid table name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        validate_table_name("")
    assert "required" in str(excinfo.value)
