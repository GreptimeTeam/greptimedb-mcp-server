import pytest
import json

from greptimedb_mcp_server.config import Config
from greptimedb_mcp_server import server
from greptimedb_mcp_server.server import (
    AppState,
    execute_sql,
    describe_table,
    health_check,
    execute_tql,
    query_range,
    explain_query,
    read_table_resource,
    list_pipelines,
    create_pipeline,
    dryrun_pipeline,
    delete_pipeline,
    _validate_pipeline_name,
)
from greptimedb_mcp_server.utils import templates_loader


@pytest.fixture(autouse=True)
def setup_state():
    """Initialize application state for tests."""
    config = Config(
        host="localhost",
        port=4002,
        user="testuser",
        password="testpassword",
        database="testdb",
        time_zone="",
        pool_size=5,
        http_port=4000,
        mask_enabled=False,  # Disable masking for tests
        mask_patterns="",
    )
    db_config = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "database": config.database,
        "time_zone": config.time_zone,
    }
    pool_config = {
        "pool_name": "greptimedb_pool",
        "pool_size": 5,
        "pool_reset_session": True,
        **db_config,
    }

    server._state = AppState(
        db_config=db_config,
        pool_config=pool_config,
        templates=templates_loader(),
        http_base_url=f"http://{config.host}:{config.http_port}",
        mask_enabled=config.mask_enabled,
        mask_patterns=[],
    )

    yield

    server._state = None


@pytest.mark.asyncio
async def test_execute_sql_select():
    """Test executing a SELECT query"""
    result = await execute_sql(query="SELECT * FROM users")

    assert "id,name" in result
    assert "1," in result
    assert "John" in result


@pytest.mark.asyncio
async def test_execute_sql_dangerous_blocked():
    """Test security gate blocking dangerous queries"""
    result = await execute_sql(query="DROP TABLE users")

    assert "Error: Dangerous operation blocked" in result
    assert "Forbidden `DROP` operation" in result


@pytest.mark.asyncio
async def test_execute_sql_show_tables():
    """Test SHOW TABLES query execution"""
    result = await execute_sql(query="SHOW TABLES")

    assert "table_name" in result  # Uses actual column name from cursor.description
    assert "users" in result
    assert "orders" in result


@pytest.mark.asyncio
async def test_execute_sql_show_databases():
    """Test SHOW DATABASES query execution"""
    result = await execute_sql(query="SHOW DATABASES")

    assert "Databases" in result
    assert "public" in result
    assert "greptime_private" in result


@pytest.mark.asyncio
async def test_execute_sql_json_format():
    """Test execute_sql with JSON format"""
    result = await execute_sql(query="SELECT * FROM users", format="json")

    data = json.loads(result)
    assert "data" in data
    assert "row_count" in data
    assert "execution_time_ms" in data
    assert len(data["data"]) == 2


@pytest.mark.asyncio
async def test_execute_sql_markdown_format():
    """Test execute_sql with markdown format"""
    result = await execute_sql(query="SELECT * FROM users", format="markdown")

    assert "|" in result
    assert "id" in result
    assert "name" in result
    assert "---" in result


@pytest.mark.asyncio
async def test_execute_sql_missing_query():
    """Test execute_sql with missing query parameter"""
    with pytest.raises(ValueError) as excinfo:
        await execute_sql(query="")
    assert "Query is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_sql_invalid_format():
    """Test execute_sql with invalid format parameter"""
    with pytest.raises(ValueError) as excinfo:
        await execute_sql(query="SELECT 1", format="xml")
    assert "Invalid format" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_sql_with_limit():
    """Test execute_sql with limit parameter"""
    result = await execute_sql(query="SELECT * FROM users", limit=1)
    assert "id" in result or "name" in result


@pytest.mark.asyncio
async def test_execute_sql_union_allowed():
    """Test execute_sql allows UNION queries"""
    result = await execute_sql(query="SELECT * FROM users UNION SELECT * FROM admins")
    assert "id" in result or "name" in result


@pytest.mark.asyncio
async def test_describe_table():
    """Test describe_table tool"""
    result = await describe_table(table="users")

    assert "Column" in result
    assert "Type" in result
    assert "|" in result


@pytest.mark.asyncio
async def test_describe_table_invalid_name():
    """Test describe_table with invalid table name"""
    with pytest.raises(ValueError) as excinfo:
        await describe_table(table="123invalid")
    assert "Invalid table name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_describe_table_missing_table():
    """Test describe_table with missing table parameter"""
    with pytest.raises(ValueError) as excinfo:
        await describe_table(table="")
    assert "Table name is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_health_check():
    """Test health_check tool"""
    result = await health_check()

    health = json.loads(result)
    assert health["status"] == "healthy"
    assert health["host"] == "localhost"
    assert health["port"] == 4002
    assert "version" in health
    assert "response_time_ms" in health


@pytest.mark.asyncio
async def test_execute_tql():
    """Test execute_tql tool"""
    result = await execute_tql(
        query="rate(http_requests_total[5m])",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T01:00:00Z",
        step="1m",
    )

    data = json.loads(result)
    assert "tql" in data
    assert "data" in data
    assert "row_count" in data
    assert "TQL EVAL" in data["tql"]


@pytest.mark.asyncio
async def test_execute_tql_with_lookback():
    """Test execute_tql with optional lookback parameter"""
    result = await execute_tql(
        query="rate(x[5m])",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T01:00:00Z",
        step="1m",
        lookback="5m",
    )

    data = json.loads(result)
    assert "tql" in data
    assert "5m" in data["tql"]


@pytest.mark.asyncio
async def test_execute_tql_missing_params():
    """Test execute_tql with missing required parameters"""
    with pytest.raises(ValueError) as excinfo:
        await execute_tql(query="rate(x[5m])", start="", end="", step="")
    assert "required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_tql_injection_blocked():
    """Test execute_tql blocks injection in parameters"""
    with pytest.raises(ValueError) as excinfo:
        await execute_tql(
            query="rate(x[5m])",
            start="2024-01-01'; DROP TABLE users; --",
            end="2024-01-01T01:00:00Z",
            step="1m",
        )
    assert "Invalid characters" in str(excinfo.value)


@pytest.mark.asyncio
async def test_execute_tql_dangerous_query_blocked():
    """Test execute_tql blocks dangerous patterns in query"""
    result = await execute_tql(
        query="rate(x[5m]); DROP TABLE users",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T01:00:00Z",
        step="1m",
    )
    assert "Error: Dangerous operation blocked" in result


@pytest.mark.asyncio
async def test_query_range():
    """Test query_range tool"""
    result = await query_range(
        table="metrics",
        select="ts, host, avg(cpu) RANGE '5m'",
        align="1m",
        by="host",
    )

    data = json.loads(result)
    assert "query" in data
    assert "data" in data
    assert "ALIGN" in data["query"]


@pytest.mark.asyncio
async def test_query_range_with_where():
    """Test query_range with where condition"""
    result = await query_range(
        table="metrics",
        select="ts, avg(cpu)",
        align="1m",
        where="host = 'server1'",
    )

    data = json.loads(result)
    assert "query" in data
    assert "WHERE" in data["query"]


@pytest.mark.asyncio
async def test_query_range_with_by():
    """Test query_range with BY clause"""
    result = await query_range(
        table="metrics", select="ts, avg(cpu)", align="1m", by="host"
    )

    data = json.loads(result)
    assert "query" in data
    assert "BY" in data["query"]


@pytest.mark.asyncio
async def test_query_range_invalid_table():
    """Test query_range with invalid table name"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(table="123-bad", select="ts, avg(cpu)", align="1m")
    assert "Invalid table name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_missing_params():
    """Test query_range with missing required parameters"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(table="metrics", select="", align="")
    assert "required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_injection_blocked():
    """Test query_range blocks injection in where clause"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(
            table="metrics",
            select="ts, avg(cpu)",
            align="1m",
            where="1=1; DROP TABLE users; --",
        )
    assert "Dangerous pattern" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_align_injection_blocked():
    """Test query_range blocks injection in align parameter"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(
            table="metrics",
            select="ts, avg(cpu)",
            align="1m' OR '1'='1",
        )
    assert "Invalid align" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_invalid_align():
    """Test query_range rejects invalid duration format"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(
            table="metrics",
            select="ts, avg(cpu)",
            align="invalid",
        )
    assert "Invalid align" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_fill_injection_blocked():
    """Test query_range blocks injection in fill parameter"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(
            table="metrics",
            select="ts, avg(cpu)",
            align="1m",
            fill="NULL; DROP TABLE users; --",
        )
    assert "Invalid fill" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_valid_fill():
    """Test query_range accepts valid fill values"""
    # Test NULL
    result = await query_range(
        table="metrics", select="ts, avg(cpu)", align="1m", fill="NULL"
    )
    data = json.loads(result)
    assert "FILL NULL" in data["query"]

    # Test numeric
    result = await query_range(
        table="metrics", select="ts, avg(cpu)", align="1m", fill="0"
    )
    data = json.loads(result)
    assert "FILL 0" in data["query"]


@pytest.mark.asyncio
async def test_explain_query():
    """Test explain_query tool"""
    result = await explain_query(query="SELECT * FROM users")

    assert "plan" in result
    assert "|" in result


@pytest.mark.asyncio
async def test_explain_query_with_analyze():
    """Test explain_query with analyze=true"""
    result = await explain_query(query="SELECT * FROM users", analyze=True)

    assert "plan" in result or "|" in result


@pytest.mark.asyncio
async def test_explain_query_missing_query():
    """Test explain_query with missing query parameter"""
    with pytest.raises(ValueError) as excinfo:
        await explain_query(query="")
    assert "query is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_explain_query_dangerous_blocked():
    """Test explain_query blocks dangerous queries"""
    result = await explain_query(query="DROP TABLE users")
    assert "Error: Dangerous operation blocked" in result


@pytest.mark.asyncio
async def test_read_table_resource():
    """Test reading a table resource"""
    result = await read_table_resource(table="users")

    assert "id,name" in result
    assert "1,John" in result
    assert "2,Jane" in result


@pytest.mark.asyncio
async def test_read_table_resource_invalid_name():
    """Test read_table_resource with invalid table name"""
    with pytest.raises(ValueError) as excinfo:
        await read_table_resource(table="123invalid")
    assert "Invalid table name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_describe_table_schema_qualified():
    """Test describe_table with schema.table format"""
    result = await describe_table(table="public.users")
    assert "Column" in result
    assert "Type" in result


@pytest.mark.asyncio
async def test_execute_tql_csv_format():
    """Test execute_tql with CSV format"""
    result = await execute_tql(
        query="rate(http_requests_total[5m])",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T01:00:00Z",
        step="1m",
        format="csv",
    )
    # CSV format returns raw CSV, not JSON wrapper
    assert "ts" in result or "value" in result


@pytest.mark.asyncio
async def test_execute_tql_invalid_format():
    """Test execute_tql with invalid format parameter"""
    with pytest.raises(ValueError) as excinfo:
        await execute_tql(
            query="rate(x[5m])",
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T01:00:00Z",
            step="1m",
            format="xml",
        )
    assert "Invalid format" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_csv_format():
    """Test query_range with CSV format"""
    result = await query_range(
        table="metrics",
        select="ts, avg(cpu)",
        align="1m",
        format="csv",
    )
    # CSV format returns raw CSV, not JSON wrapper
    assert "ts" in result or "avg" in result


@pytest.mark.asyncio
async def test_query_range_invalid_format():
    """Test query_range with invalid format parameter"""
    with pytest.raises(ValueError) as excinfo:
        await query_range(
            table="metrics",
            select="ts, avg(cpu)",
            align="1m",
            format="xml",
        )
    assert "Invalid format" in str(excinfo.value)


@pytest.mark.asyncio
async def test_query_range_schema_qualified_table():
    """Test query_range with schema.table format"""
    result = await query_range(
        table="public.metrics",
        select="ts, avg(cpu)",
        align="1m",
    )
    data = json.loads(result)
    assert "query" in data
    assert "public.metrics" in data["query"]


# Pipeline tools tests


def test_validate_pipeline_name_valid():
    """Test valid pipeline names"""
    assert _validate_pipeline_name("test_pipeline") == "test_pipeline"
    assert _validate_pipeline_name("Pipeline1") == "Pipeline1"
    assert _validate_pipeline_name("_private") == "_private"
    assert _validate_pipeline_name("a") == "a"


def test_validate_pipeline_name_invalid():
    """Test invalid pipeline names"""
    with pytest.raises(ValueError) as excinfo:
        _validate_pipeline_name("")
    assert "Pipeline name is required" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        _validate_pipeline_name("123invalid")
    assert "Invalid pipeline name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        _validate_pipeline_name("test-pipeline")
    assert "Invalid pipeline name" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        _validate_pipeline_name("test.pipeline")
    assert "Invalid pipeline name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_list_pipelines():
    """Test list_pipelines tool"""
    result = await list_pipelines()
    # Since we're using mocked DB, check for expected output format
    assert "No pipelines found." in result or "name" in result


@pytest.mark.asyncio
async def test_list_pipelines_with_name():
    """Test list_pipelines with specific name filter"""
    result = await list_pipelines(name="test_pipeline")
    assert "No pipelines found." in result or "name" in result


@pytest.mark.asyncio
async def test_create_pipeline_invalid_name():
    """Test create_pipeline with invalid name"""
    with pytest.raises(ValueError) as excinfo:
        await create_pipeline(name="123-invalid", pipeline="version: 2")
    assert "Invalid pipeline name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_dryrun_pipeline_invalid_name():
    """Test dryrun_pipeline with invalid name"""
    with pytest.raises(ValueError) as excinfo:
        await dryrun_pipeline(pipeline_name="123-invalid", data='{"message": "test"}')
    assert "Invalid pipeline name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_dryrun_pipeline_invalid_json():
    """Test dryrun_pipeline with invalid JSON data"""
    result = await dryrun_pipeline(pipeline_name="test_pipeline", data="invalid json")
    assert "Error: Invalid JSON data" in result


@pytest.mark.asyncio
async def test_delete_pipeline_invalid_name():
    """Test delete_pipeline with invalid name"""
    with pytest.raises(ValueError) as excinfo:
        await delete_pipeline(name="123-invalid", version="2024-01-01")
    assert "Invalid pipeline name" in str(excinfo.value)


@pytest.mark.asyncio
async def test_delete_pipeline_missing_version():
    """Test delete_pipeline with missing version"""
    result = await delete_pipeline(name="test_pipeline", version="")
    assert "Error: version is required" in result
