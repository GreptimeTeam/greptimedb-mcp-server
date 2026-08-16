"""Black-box tests: real MCP client -> real server subprocess -> real GreptimeDB.

Each test opens its own stdio session. The session is entered inside the test
body rather than in a fixture because unwinding anyio cancel scopes during
pytest-asyncio fixture finalization happens on a different task and fails.
"""

import json

import pytest
from pydantic import AnyUrl

from .conftest import (
    CREDENTIALS_TABLE,
    MASK_PLACEHOLDER,
    METRICS_TABLE,
    SECRET_API_KEY,
    SECRET_PASSWORD,
    call_text,
    stdio_session,
)

pytestmark = pytest.mark.integration


async def test_tool_catalog(seed):
    async with stdio_session() as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
    assert {
        "execute_sql",
        "describe_table",
        "health_check",
        "execute_tql",
        "query_range",
        "explain_query",
        "list_pipelines",
        "create_pipeline",
        "dryrun_pipeline",
        "delete_pipeline",
        "list_dashboards",
        "create_dashboard",
        "delete_dashboard",
    } <= tools


async def test_health_check(seed):
    async with stdio_session() as client:
        result = json.loads(await call_text(client, "health_check"))
    assert result["status"] == "healthy"
    assert "GreptimeDB" in result["version"]


async def test_execute_sql_json(seed):
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "execute_sql",
                {
                    "query": f"SELECT count(*) AS n FROM {METRICS_TABLE}",
                    "format": "json",
                },
            )
        )
    assert payload["data"][0]["n"] == seed.row_count


async def test_execute_sql_csv_and_markdown(seed):
    query = f"SELECT host, cpu FROM {METRICS_TABLE} LIMIT 1"
    async with stdio_session() as client:
        csv_out = await call_text(
            client, "execute_sql", {"query": query, "format": "csv"}
        )
        md_out = await call_text(
            client, "execute_sql", {"query": query, "format": "markdown"}
        )

    assert csv_out.splitlines()[0] == "host,cpu"
    assert md_out.lstrip().startswith("|")
    assert "host" in md_out


async def test_execute_sql_limit_truncates(seed):
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "execute_sql",
                {
                    "query": f"SELECT * FROM {METRICS_TABLE}",
                    "format": "json",
                    "limit": 5,
                },
            )
        )
    assert payload["row_count"] == 5
    assert payload["truncated"] is True


async def test_read_only_server_blocks_ddl(seed, db):
    async with stdio_session() as client:
        output = await call_text(
            client, "execute_sql", {"query": f"DROP TABLE {METRICS_TABLE}"}
        )
    assert "Dangerous operation blocked" in output

    cursor = db.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{METRICS_TABLE}'")
    assert cursor.fetchall(), "security gate let the DROP through"


async def test_allow_write_permits_ddl(seed, db):
    table = "it_write_mode_probe"
    async with stdio_session(**{"--allow-write": "true"}) as client:
        await call_text(
            client,
            "execute_sql",
            {"query": f"CREATE TABLE {table} (ts TIMESTAMP TIME INDEX, v DOUBLE)"},
        )

    cursor = db.cursor()
    try:
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        assert cursor.fetchall(), "write mode did not create the table"
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        db.commit()


async def test_describe_table(seed):
    async with stdio_session() as client:
        profile = json.loads(
            await call_text(client, "describe_table", {"table": METRICS_TABLE})
        )
    assert profile["table_name"] == METRICS_TABLE
    assert profile["schema"]["time_index"] == "ts"
    columns = {column["name"] for column in profile["schema"]["columns"]}
    assert {"ts", "host", "cpu"} <= columns
    assert profile["samples"]["included"] is True
    assert profile["samples"]["rows"]


async def test_describe_missing_table(seed):
    async with stdio_session() as client:
        profile = json.loads(
            await call_text(client, "describe_table", {"table": "it_does_not_exist"})
        )
    assert "not found" in profile["error"]


async def test_query_range(seed):
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "query_range",
                {
                    "table": METRICS_TABLE,
                    "select": "ts, host, avg(cpu) RANGE '30s'",
                    "align": "30s",
                    "by": "host",
                    "order_by": "ts DESC",
                    "format": "json",
                },
            )
        )
    assert payload["row_count"] > 0
    assert "ALIGN '30s'" in payload["query"]


async def test_execute_tql(seed):
    start = seed.earliest_ms // 1000 - 60
    end = seed.base_ms // 1000 + 60
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "execute_tql",
                {
                    "query": METRICS_TABLE,
                    "start": str(start),
                    "end": str(end),
                    "step": "30s",
                    "format": "json",
                },
            )
        )
    assert payload["row_count"] > 0
    assert payload["tql"].startswith("TQL EVAL")


async def test_explain_query(seed):
    async with stdio_session() as client:
        plan = await call_text(
            client, "explain_query", {"query": f"SELECT * FROM {METRICS_TABLE}"}
        )
    assert "TableScan" in plan
    assert METRICS_TABLE in plan


async def test_explain_analyze_verbose_reports_metrics(seed):
    async with stdio_session() as client:
        plan = await call_text(
            client,
            "explain_query",
            {
                "query": f"SELECT * FROM {METRICS_TABLE}",
                "analyze": True,
                "verbose": True,
            },
        )
    assert "output_rows" in plan


async def test_read_table_resource(seed):
    async with stdio_session() as client:
        result = await client.read_resource(AnyUrl(f"greptime://{METRICS_TABLE}/data"))
    body = result.contents[0].text
    assert body.splitlines()[0] == "ts,host,cpu"
    assert len(body.splitlines()) == seed.row_count + 1


async def test_masking_hides_sensitive_columns(seed):
    query = f"SELECT * FROM {CREDENTIALS_TABLE}"
    async with stdio_session() as client:
        output = await call_text(
            client, "execute_sql", {"query": query, "format": "csv"}
        )
    assert SECRET_PASSWORD not in output
    assert SECRET_API_KEY not in output
    assert MASK_PLACEHOLDER in output
    assert "alice" in output, "non-sensitive columns must survive masking"


async def test_masking_can_be_disabled(seed):
    query = f"SELECT * FROM {CREDENTIALS_TABLE}"
    async with stdio_session(**{"--mask-enabled": "false"}) as client:
        output = await call_text(
            client, "execute_sql", {"query": query, "format": "csv"}
        )
    assert SECRET_PASSWORD in output
    assert SECRET_API_KEY in output


PIPELINE_YAML = """processors:
  - date:
      field: time
      formats:
        - "%Y-%m-%d %H:%M:%S%.3f"
      ignore_missing: true
transform:
  - fields:
      - id
    type: int32
  - field: time
    type: time
    index: timestamp
"""


async def test_pipeline_lifecycle(seed):
    name = "it_lifecycle_pipeline"
    async with stdio_session() as client:
        created = await call_text(
            client, "create_pipeline", {"name": name, "pipeline": PIPELINE_YAML}
        )
        assert "created successfully" in created
        version = created.rsplit("Version:", 1)[1].strip()

        try:
            listed = await call_text(client, "list_pipelines", {"name": name})
            assert name in listed

            dryrun = json.loads(
                await call_text(
                    client,
                    "dryrun_pipeline",
                    {
                        "pipeline_name": name,
                        "data": json.dumps(
                            [{"id": 1, "time": "2024-05-25 20:16:37.217"}]
                        ),
                    },
                )
            )
            assert dryrun[0]["rows"]
        finally:
            deleted = await call_text(
                client, "delete_pipeline", {"name": name, "version": version}
            )
    assert "deleted successfully" in deleted


async def test_dashboard_lifecycle(seed):
    name = "it_lifecycle_dashboard"
    definition = json.dumps(
        {"kind": "Dashboard", "metadata": {"name": name}, "spec": {"panels": {}}}
    )
    async with stdio_session() as client:
        created = await call_text(
            client, "create_dashboard", {"name": name, "definition": definition}
        )
        assert "saved successfully" in created

        try:
            listed = json.loads(await call_text(client, "list_dashboards"))
            assert any(d["name"] == name for d in listed["dashboards"])
        finally:
            deleted = await call_text(client, "delete_dashboard", {"name": name})
    assert "deleted successfully" in deleted


async def test_prompts_render(seed):
    async with stdio_session() as client:
        prompts = {prompt.name for prompt in (await client.list_prompts()).prompts}
        assert "table_operation" in prompts
        rendered = await client.get_prompt("table_operation", {"table": METRICS_TABLE})
    assert METRICS_TABLE in rendered.messages[0].content.text
