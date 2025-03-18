
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
        database="testdb"
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
    assert "query" in tools[0].inputSchema['properties']

@pytest.mark.asyncio
async def test_call_tool_select_query(logger, config):
    """Test executing a SELECT query via tool"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_sql",
        {"query": "SELECT * FROM users"}
    )

    # Verify the results
    assert len(result) == 1
    assert "id,name" in result[0].text
    assert "1,John" in result[0].text

@pytest.mark.asyncio
async def test_security_gate_dangerous_query(logger, config):
    """Test security gate blocking dangerous queries"""
    server = DatabaseServer(logger, config)

    result = await server.call_tool(
        "execute_sql",
        {"query": "DROP TABLE users"}
    )

    # Verify that the security gate blocked the query
    assert "Error: Contain dangerous operations" in result[0].text
    assert "Forbided `DROP` operation" in result[0].text

@pytest.mark.asyncio
async def test_show_tables_query(logger, config):
    """Test SHOW TABLES query execution"""
    server = DatabaseServer(logger, config)
    result = await server.call_tool(
        "execute_sql",
        {"query": "SHOW TABLES"}
    )

    # Verify the results
    assert len(result) == 1
    assert "Tables_in_testdb" in result[0].text
    assert "users" in result[0].text
    assert "orders" in result[0].text

def test_server_initialization(logger, config):
    """Test server initialization with configuration"""
    server = DatabaseServer(logger, config)

    # Verify the server was initialized correctly
    assert server.logger == logger
    assert server.db_config['host'] == 'localhost'
    assert server.db_config['port'] == 4002
    assert server.db_config['user'] == 'testuser'
    assert server.db_config['password'] == 'testpassword'
    assert server.db_config['database'] == 'testdb'
