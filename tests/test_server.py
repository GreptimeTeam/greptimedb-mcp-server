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
    assert "1,John" in result
    assert "2,Jane" in result


@pytest.mark.asyncio
async def test_list_tools(logger, config):
    """Test listing available database tools"""
    server = DatabaseServer(logger, config)
    tools = await server.list_tools()

    # Verify the tool list
    assert len(tools) == 1
    assert tools[0].name == "execute_sql"
    assert "query" in tools[0].inputSchema["properties"]


@pytest.mark.asyncio
async def test_call_tool_select_query(logger, config):
    """Test executing a SELECT query via tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool("execute_sql", {"query": "SELECT * FROM users"})

    # Verify the results
    assert len(result) == 1
    assert "id,name" in result[0].text
    assert "1,John" in result[0].text


@pytest.mark.asyncio
async def test_security_gate_dangerous_query(logger, config):
    """Test security gate blocking dangerous queries"""
    server = DatabaseServer(logger, config)

    result = await server.call_tool("execute_sql", {"query": "DROP TABLE users"})

    # Verify that the security gate blocked the query
    assert "Error: Contain dangerous operations" in result[0].text
    assert "Forbided `DROP` operation" in result[0].text


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
