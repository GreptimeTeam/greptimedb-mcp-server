"""Black-box tests: real MCP client -> real server subprocess -> real GreptimeDB.

Sessions are entered in the test body rather than in a fixture: anyio cancel
scopes cannot be unwound from pytest-asyncio's finalization task.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from mcp import MCPError

from .conftest import (
    CREDENTIALS_TABLE,
    MASK_PLACEHOLDER,
    GRAPH_EDGES,
    METRICS_TABLE,
    SECRET_API_KEY,
    SECRET_PASSWORD,
    call_text,
    stdio_session,
)

pytestmark = pytest.mark.integration


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


async def test_invalid_argument_reports_the_reason(seed):
    """The SDK reports a plain exception with a generic message, so the tool
    and resource wrappers must translate validation failures."""
    async with stdio_session() as client:
        result = await client.call_tool(
            "execute_sql", {"query": "SELECT 1", "format": "xml"}
        )
        assert result.is_error
        assert "Invalid format" in result.content[0].text

        with pytest.raises(MCPError) as excinfo:
            await client.read_resource("greptime://123invalid/data")
        assert "Invalid table name" in str(excinfo.value)


async def test_read_table_resource(seed):
    async with stdio_session() as client:
        result = await client.read_resource(f"greptime://{METRICS_TABLE}/data")
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
        # Without a version the pipeline cannot be deleted, so it would leak.
        assert "created successfully" in created and "Version:" in created
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


async def test_search_table_semantics(seed):
    """Terms are ORed: a table matching only one of them is still a candidate."""
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "search_table_semantics",
                {"query": "cpu saturation percentile"},
            )
        )

    assert payload["available"] is True
    match = next(m for m in payload["matches"] if m["table"] == METRICS_TABLE)
    assert match["matched_terms"] == ["cpu"]
    assert match["signal_type"] == "metric"
    assert match["semantic_options"]["metric.type"] == "gauge"


async def test_describe_table_reports_entity_declarations(seed):
    async with stdio_session() as client:
        profile = json.loads(
            await call_text(
                client,
                "describe_table",
                {"table": METRICS_TABLE, "include_samples": False},
            )
        )

    semantics = profile["semantics"]
    assert semantics["found"] is True
    assert semantics["signal_type"] == "metric"

    if seed.entity_declared:
        types = {d["entity_type"] for d in semantics["entity_declarations"]}
        assert "host" in types
        assert any("id_qualifier" in line for line in profile["guidance"])
    else:
        # Older GreptimeDB: the column is absent and must be reported as a
        # version limit rather than as "this table declares nothing".
        assert semantics["missing_columns"] == ["entity_declarations"]
        assert any(
            "does not expose entity_declarations" in line
            for line in profile["guidance"]
        )


def _graph_window(seed):
    """A window wide enough to contain the seeded edges."""
    base = datetime.fromtimestamp(seed.base_ms / 1000, tz=timezone.utc)
    return {
        "start_time": (base - timedelta(minutes=30)).isoformat(),
        "end_time": (base + timedelta(minutes=30)).isoformat(),
    }


async def test_graph_tool_is_withdrawn_without_a_graph(seed):
    """A server whose database has no graph must not advertise the tool."""
    async with stdio_session() as client:
        names = {tool.name for tool in (await client.list_tools()).tools}

    assert ("query_semantic_graph" in names) is seed.graph_seeded


async def test_graph_summary_reports_shape_without_returning_edges(seed):
    if not seed.graph_seeded:
        pytest.skip("this GreptimeDB has no semantic graph")

    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "query_semantic_graph",
                {"view": "summary", **_graph_window(seed)},
            )
        )

    by_type = {item["type"]: item for item in payload["relationship_types"]}
    assert by_type["calls"]["count"] == 2
    assert by_type["calls"]["endpoints"] == [
        {"source": "service", "destination": "service", "count": 2}
    ]
    # Pairs, not two sets: service->pod and pod->node must not read as
    # service->node.
    assert by_type["runs_on"]["endpoints"] == [
        {"source": "k8s.pod", "destination": "k8s.node", "count": 1},
        {"source": "service", "destination": "k8s.pod", "count": 1},
    ]
    assert "items" not in payload


async def test_graph_window_is_read_in_utc_whatever_the_session(seed):
    """A literal without an offset is read in the session time zone."""
    if not seed.graph_seeded:
        pytest.skip("this GreptimeDB has no semantic graph")

    window = _graph_window(seed)
    async with stdio_session(**{"--timezone": "Asia/Shanghai"}) as client:
        payload = json.loads(
            await call_text(
                client, "query_semantic_graph", {"view": "summary", **window}
            )
        )

    assert payload["relationship_count"] == len(GRAPH_EDGES)


async def test_graph_returns_identifiers_verbatim(seed):
    """A caller has to be able to feed a returned id straight back in."""
    if not seed.graph_seeded:
        pytest.skip("this GreptimeDB has no semantic graph")

    window = _graph_window(seed)
    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "query_semantic_graph",
                {"view": "relationships", "rel_type": "calls", **window},
            )
        )
        first = payload["items"][0]
        refetched = json.loads(
            await call_text(
                client,
                "query_semantic_graph",
                {
                    "view": "relationships",
                    "src_id": first["src_id"],
                    "dst_id": first["dst_id"],
                    **window,
                },
            )
        )

    assert isinstance(first["src_id"], str)
    assert refetched["item_count"] == 1


async def test_graph_orders_mixed_relationships_by_type_not_red(seed):
    """Only `calls` has RED; sorting a mixed result by it buries the rest."""
    if not seed.graph_seeded:
        pytest.skip("this GreptimeDB has no semantic graph")

    window = _graph_window(seed)
    async with stdio_session() as client:
        mixed = json.loads(
            await call_text(
                client, "query_semantic_graph", {"view": "relationships", **window}
            )
        )
        calls = json.loads(
            await call_text(
                client,
                "query_semantic_graph",
                {"view": "relationships", "rel_type": "calls", **window},
            )
        )

    assert [item["rel_type"] for item in mixed["items"]] == sorted(
        item["rel_type"] for item in mixed["items"]
    )
    assert [item["error_count"] for item in calls["items"]] == [45, 7]


async def test_graph_zero_result_keeps_the_envelope(seed):
    if not seed.graph_seeded:
        pytest.skip("this GreptimeDB has no semantic graph")

    async with stdio_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "query_semantic_graph",
                {
                    "view": "relationships",
                    "rel_type": "calls",
                    "src_id": "not-a-graph-id",
                    **_graph_window(seed),
                },
            )
        )

    assert payload["status"] == "no_match"
    assert payload["items"] == []
    assert payload["applied_filters"]["rel_type"] == "calls"

    # The suggested retry drops the identifier, keeps the type filter, and
    # carries the window, so a caller can run it as given.
    next_query = payload["guidance"]["next_query"]
    assert "src_id" not in next_query
    assert next_query["rel_type"] == "calls"

    async with stdio_session() as client:
        retried = json.loads(
            await call_text(client, "query_semantic_graph", next_query)
        )
    assert retried["status"] == "ok"
