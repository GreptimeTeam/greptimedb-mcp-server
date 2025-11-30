import pytest
import datetime
import json
from greptimedb_mcp_server.utils import templates_loader, security_gate
from greptimedb_mcp_server.server import format_value, format_results


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


def test_empty_queries():
    """Test empty queries"""
    assert security_gate("") == (True, "Empty query not allowed")
    assert security_gate("   ") == (True, "Empty query not allowed")
    assert security_gate(None) == (True, "Empty query not allowed")


def test_safe_queries():
    """Test safe queries"""
    assert security_gate("SELECT * FROM users") == (False, "")
    assert security_gate("select id from products") == (False, "")


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
    """Test multiple statements"""
    assert security_gate("SELECT * FROM users; SELECT * FROM test") == (
        True,
        "Forbidden multiple statements",
    )


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


# ============================================================
# format_value Tests
# ============================================================


def test_format_value_string():
    """Test format_value with string input"""
    assert format_value("hello") == '"hello"'


def test_format_value_int():
    """Test format_value with integer input"""
    assert format_value(42) == "42"


def test_format_value_float():
    """Test format_value with float input"""
    assert format_value(3.14) == "3.14"


def test_format_value_datetime():
    """Test format_value with datetime input"""
    dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
    assert format_value(dt) == '"2024-01-01 12:00:00"'


def test_format_value_date():
    """Test format_value with date input"""
    d = datetime.date(2024, 1, 1)
    assert format_value(d) == '"2024-01-01"'


def test_format_value_none():
    """Test format_value with None input"""
    assert format_value(None) == "None"


# ============================================================
# format_results Tests
# ============================================================


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


# ============================================================
# Security Gate Extension Tests
# ============================================================


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


def test_security_gate_desc():
    """Test security gate allows DESC/DESCRIBE queries"""
    result = security_gate("DESCRIBE users")
    assert result[0] is False
