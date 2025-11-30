import pytest
import logging

# Now we can safely import these
from greptimedb_mcp_server.server import DatabaseServer
from greptimedb_mcp_server.config import Config


@pytest.fixture
def config():
    """Create a test configuration"""
    return Config(
        host="localhost",
        port=4002,
        user="testuser",
        password="testpassword",
        database="testdb",
        time_zone="",
    )


@pytest.fixture
def logger():
    """Create a test logger"""
    return logging.getLogger("test_logger")


@pytest.mark.asyncio
async def test_list_resources(logger, config):
    """Test listing database resources"""
    server = DatabaseServer(logger, config)
    resources = await server.list_resources()

    # Verify the results
    assert len(resources) == 2
    assert resources[0].name == "Table: users"
    assert str(resources[0].uri) == "greptime://users/data"


@pytest.mark.asyncio
async def test_read_resource(logger, config):
    """Test reading a specific database resource"""
    server = DatabaseServer(logger, config)
    result = await server.read_resource("greptime://users/data")

    # Verify the results contain expected data
    assert "id,name" in result
    assert '1,"John"' in result
    assert '2,"Jane"' in result


@pytest.mark.asyncio
async def test_list_tools(logger, config):
    """Test listing available database tools"""
    server = DatabaseServer(logger, config)
    tools = await server.list_tools()

    # Verify the tool list (6 tools total)
    assert len(tools) == 6
    tool_names = [t.name for t in tools]
    assert "execute_sql" in tool_names
    assert "describe_table" in tool_names
    assert "health_check" in tool_names
    assert "execute_tql" in tool_names
    assert "query_range" in tool_names
    assert "explain_query" in tool_names

    # Check execute_sql has new parameters
    execute_sql = next(t for t in tools if t.name == "execute_sql")
    assert "query" in execute_sql.inputSchema["properties"]
    assert "format" in execute_sql.inputSchema["properties"]
    assert "limit" in execute_sql.inputSchema["properties"]


@pytest.mark.asyncio
async def test_call_tool_select_query(logger, config):
    """Test executing a SELECT query via tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("execute_sql", {"query": "SELECT * FROM users"})

    # Verify the results
    assert len(result) == 1
    assert "id,name" in result[0].text
    assert "1," in result[0].text
    assert "John" in result[0].text


@pytest.mark.asyncio
async def test_security_gate_dangerous_query(logger, config):
    """Test security gate blocking dangerous queries"""
    server = DatabaseServer(logger, config)

    result = await server.call_tool("execute_sql", {"query": "DROP TABLE users"})

    # Verify that the security gate blocked the query
    assert "Error: Contain dangerous operations" in result[0].text
    assert "Forbidden `DROP` operation" in result[0].text


@pytest.mark.asyncio
async def test_show_tables_query(logger, config):
    """Test SHOW TABLES query execution"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("execute_sql", {"query": "SHOW TABLES"})

    # Verify the results
    assert len(result) == 1
    assert "Tables_in_testdb" in result[0].text
    assert "users" in result[0].text
    assert "orders" in result[0].text


@pytest.mark.asyncio
async def test_show_dbs_query(logger, config):
    """Test SHOW DATABASES query execution"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("execute_sql", {"query": "SHOW DATABASES"})

    # Verify the results
    assert len(result) == 1
    assert "Databases" in result[0].text
    print(result[0].text)
    assert "public" in result[0].text
    assert "greptime_private" in result[0].text


@pytest.mark.asyncio
async def test_list_prompts(logger, config):
    """Test listing available prompts"""
    server = DatabaseServer(logger, config)
    prompts = await server.list_prompts()

    # Verify the results
    assert len(prompts) > 0
    # Check that each prompt has the expected properties
    for prompt in prompts:
        assert hasattr(prompt, "name")
        assert hasattr(prompt, "description")
        assert hasattr(prompt, "arguments")


@pytest.mark.asyncio
async def test_get_prompt_without_args(logger, config):
    """Test getting a prompt without arguments"""
    server = DatabaseServer(logger, config)
    # Get the first prompt from the list to test with
    prompts = await server.list_prompts()
    if not prompts:
        pytest.skip("No prompts available for testing")

    test_prompt_name = prompts[0].name
    result = await server.get_prompt(test_prompt_name, {})

    # Verify the result has the expected structure
    assert hasattr(result, "messages")
    assert len(result.messages) > 0
    for message in result.messages:
        assert hasattr(message, "role")
        assert hasattr(message, "content")


@pytest.mark.asyncio
async def test_get_prompt_with_args(logger, config):
    """Test getting a prompt with argument substitution"""
    server = DatabaseServer(logger, config)
    # Assume there's a prompt with arguments
    prompts = await server.list_prompts()
    prompt_with_args = None

    # Find a prompt that has arguments
    for prompt in prompts:
        if prompt.arguments and len(prompt.arguments) > 0:
            prompt_with_args = prompt
            break

    if not prompt_with_args:
        pytest.skip("No prompts with arguments available for testing")

    # Create args dictionary with test values for each required argument
    args = {}
    for arg in prompt_with_args.arguments:
        args[arg.name] = f"test_{arg.name}"

    result = await server.get_prompt(prompt_with_args.name, args)

    # Verify result structure and argument substitution
    assert hasattr(result, "messages")
    assert len(result.messages) > 0

    # Check that at least one message contains our test values
    substitution_found = False
    for message in result.messages:
        for arg_name, arg_value in args.items():
            if arg_value in message.content.text:
                substitution_found = True
                break
        if substitution_found:
            break

    assert substitution_found, "Argument substitution not found in prompt messages"


@pytest.mark.asyncio
async def test_get_prompt_nonexistent(logger, config):
    """Test getting a non-existent prompt"""
    server = DatabaseServer(logger, config)

    # Try to get a prompt that doesn't exist
    with pytest.raises(ValueError) as excinfo:
        await server.get_prompt("non_existent_prompt", {})

    # Verify the error message
    assert "Unknown template: non_existent_prompt" in str(excinfo.value)


def test_server_initialization(logger, config):
    """Test server initialization with configuration"""
    server = DatabaseServer(logger, config)

    # Verify the server was initialized correctly
    assert server.logger == logger
    assert server.db_config["host"] == "localhost"
    assert server.db_config["port"] == 4002
    assert server.db_config["user"] == "testuser"
    assert server.db_config["password"] == "testpassword"
    assert server.db_config["database"] == "testdb"


@pytest.mark.asyncio
async def test_describe_table(logger, config):
    """Test describe_table tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("describe_table", {"table": "users"})

    # Verify markdown output
    assert len(result) == 1
    assert "Column" in result[0].text
    assert "Type" in result[0].text
    assert "|" in result[0].text  # Markdown table


@pytest.mark.asyncio
async def test_health_check(logger, config):
    """Test health_check tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("health_check", {})

    # Verify JSON output
    assert len(result) == 1
    import json

    health = json.loads(result[0].text)
    assert health["status"] == "healthy"
    assert health["host"] == "localhost"
    assert health["port"] == 4002
    assert "version" in health
    assert "response_time_ms" in health


@pytest.mark.asyncio
async def test_execute_sql_json_format(logger, config):
    """Test execute_sql with JSON format"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_sql", {"query": "SELECT * FROM users", "format": "json"}
    )

    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "data" in data
    assert "row_count" in data
    assert "execution_time_ms" in data
    assert len(data["data"]) == 2


@pytest.mark.asyncio
async def test_execute_sql_markdown_format(logger, config):
    """Test execute_sql with markdown format"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_sql", {"query": "SELECT * FROM users", "format": "markdown"}
    )

    assert len(result) == 1
    # Check markdown table format
    assert "|" in result[0].text
    assert "id" in result[0].text
    assert "name" in result[0].text
    assert "---" in result[0].text


@pytest.mark.asyncio
async def test_execute_tql(logger, config):
    """Test execute_tql tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_tql",
        {
            "query": "rate(http_requests_total[5m])",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-01T01:00:00Z",
            "step": "1m",
        },
    )

    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "tql" in data
    assert "data" in data
    assert "row_count" in data
    assert "TQL EVAL" in data["tql"]


@pytest.mark.asyncio
async def test_query_range(logger, config):
    """Test query_range tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "query_range",
        {
            "table": "metrics",
            "select": "ts, host, avg(cpu) RANGE '5m'",
            "align": "1m",
            "by": "host",
        },
    )

    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "query" in data
    assert "data" in data
    assert "ALIGN" in data["query"]


@pytest.mark.asyncio
async def test_explain_query(logger, config):
    """Test explain_query tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("explain_query", {"query": "SELECT * FROM users"})

    assert len(result) == 1
    # Check markdown table format
    assert "plan" in result[0].text
    assert "|" in result[0].text


@pytest.mark.asyncio
async def test_list_tools_count(logger, config):
    """Test that all tools are listed"""
    server = DatabaseServer(logger, config)
    tools = await server.list_tools()

    # Should have 6 tools now
    assert len(tools) == 6
    tool_names = [t.name for t in tools]
    assert "execute_sql" in tool_names
    assert "describe_table" in tool_names
    assert "health_check" in tool_names
    assert "execute_tql" in tool_names
    assert "query_range" in tool_names
    assert "explain_query" in tool_names


# ============================================================
# Tool Parameter Validation Tests
# ============================================================


@pytest.mark.asyncio
async def test_describe_table_invalid_name(logger, config):
    """Test describe_table with invalid table name"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("describe_table", {"table": "123invalid"})
    assert "Invalid table name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_describe_table_missing_table(logger, config):
    """Test describe_table with missing table parameter"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("describe_table", {})
    assert "Table name is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_tql_missing_params(logger, config):
    """Test execute_tql with missing required parameters"""
    server = DatabaseServer(logger, config)
    # Missing start, end, step
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("execute_tql", {"query": "rate(x[5m])"})
    assert "required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_invalid_table(logger, config):
    """Test query_range with invalid table name"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool(
            "query_range", {"table": "123-bad", "select": "ts, avg(cpu)", "align": "1m"}
        )
    assert "Invalid table name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_missing_params(logger, config):
    """Test query_range with missing required parameters"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("query_range", {"table": "metrics"})
    assert "required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_explain_query_missing_query(logger, config):
    """Test explain_query with missing query parameter"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("explain_query", {})
    assert "query is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_sql_missing_query(logger, config):
    """Test execute_sql with missing query parameter"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("execute_sql", {})
    assert "Query is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_call_unknown_tool(logger, config):
    """Test calling an unknown tool"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.call_tool("nonexistent_tool", {})
    assert "Unknown tool" in str(excinfo.value)


# ============================================================
# Optional Parameter Tests
# ============================================================


@pytest.mark.asyncio
async def test_execute_tql_with_lookback(logger, config):
    """Test execute_tql with optional lookback parameter"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_tql",
        {
            "query": "rate(x[5m])",
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-01T01:00:00Z",
            "step": "1m",
            "lookback": "5m",
        },
    )
    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "tql" in data
    assert "lookback" in data["tql"] or "'5m'" in data["tql"]


@pytest.mark.asyncio
async def test_execute_sql_with_limit(logger, config):
    """Test execute_sql with limit parameter"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_sql", {"query": "SELECT * FROM users", "limit": 1}
    )
    assert len(result) == 1
    # Should still return data
    assert "id" in result[0].text or "name" in result[0].text


@pytest.mark.asyncio
async def test_query_range_with_where(logger, config):
    """Test query_range with where condition"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "query_range",
        {
            "table": "metrics",
            "select": "ts, avg(cpu)",
            "align": "1m",
            "where": "host = 'server1'",
        },
    )
    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "query" in data
    assert "WHERE" in data["query"]


@pytest.mark.asyncio
async def test_query_range_with_fill(logger, config):
    """Test query_range with fill parameter"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "query_range",
        {"table": "metrics", "select": "ts, avg(cpu)", "align": "1m", "by": "host"},
    )
    assert len(result) == 1
    import json

    data = json.loads(result[0].text)
    assert "query" in data
    assert "BY" in data["query"]


@pytest.mark.asyncio
async def test_explain_query_with_analyze(logger, config):
    """Test explain_query with analyze=true"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "explain_query", {"query": "SELECT * FROM users", "analyze": True}
    )
    assert len(result) == 1
    # Should return plan info
    assert "plan" in result[0].text or "|" in result[0].text


# ============================================================
# Resource Error Handling Tests
# ============================================================


@pytest.mark.asyncio
async def test_read_resource_invalid_scheme(logger, config):
    """Test read_resource with invalid URI scheme"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.read_resource("invalid://users/data")
    assert "Invalid URI scheme" in str(excinfo.value)


@pytest.mark.asyncio
async def test_read_resource_invalid_table_name(logger, config):
    """Test read_resource with invalid table name in URI"""
    server = DatabaseServer(logger, config)
    with pytest.raises(ValueError) as excinfo:
        await server.read_resource("greptime://123invalid/data")
    assert "Invalid table name" in str(excinfo.value)
