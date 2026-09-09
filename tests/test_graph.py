"""Tests for the semantic graph window, ordering, and capability contracts."""

import json
from datetime import datetime, timezone

import pytest
from mysql.connector import Error

from mcp.server.mcpserver.exceptions import ToolError

from greptimedb_mcp_server import graph, server
from greptimedb_mcp_server.graph import (
    GraphCapability,
    GraphRequest,
    GraphView,
    TimeWindow,
    _no_match_guidance,
    _relationship_order,
)

START = "2026-09-05T07:00:00Z"
END = "2026-09-05T08:00:00Z"

FULL_ENTITY_COLUMNS = frozenset(graph.ENTITY_REQUIRED_COLUMNS)
FULL_RELATIONSHIP_COLUMNS = frozenset(graph.RELATIONSHIP_REQUIRED_COLUMNS)
FULL_VIEWS = {
    graph.ENTITIES_VIEW: FULL_ENTITY_COLUMNS,
    graph.RELATIONSHIPS_VIEW: FULL_RELATIONSHIP_COLUMNS,
}


class FakeCursor:
    """Answers DESC from a column map and SELECT from a queued result."""

    def __init__(self, columns=None, rows=None, errno=None, fail_on=None):
        self.columns = columns or {}
        self.rows = rows or []
        self.errno = errno
        self.fail_on = fail_on
        self.queries = []
        self.description = None

    def execute(self, query, args=None):
        self.queries.append(query)
        if self.errno is not None and (self.fail_on is None or self.fail_on in query):
            error = Error("probe failed")
            error.errno = self.errno
            raise error
        if query.startswith("DESC TABLE"):
            view = query.split()[-1]
            self._result = [(name,) for name in sorted(self.columns.get(view, ()))]
            self.description = [("Column", None)]
        else:
            self._result = self.rows
            self.description = [("count", None)]

    def fetchall(self):
        return self._result


def request(view="relationships", limit=graph.DEFAULT_LIMIT, **filters):
    return GraphRequest.parse(view, START, END, limit, **filters)


def test_window_normalizes_to_utc():
    """A naive timestamp is read as UTC; an offset one is converted."""
    naive = TimeWindow.parse("2026-09-05T07:00:00", "2026-09-05T08:00:00")
    offset = TimeWindow.parse("2026-09-05T09:00:00+02:00", END)

    assert naive.start == datetime(2026, 9, 5, 7, tzinfo=timezone.utc)
    assert offset.start == datetime(2026, 9, 5, 7, tzinfo=timezone.utc)


def test_window_rejects_an_empty_or_inverted_range():
    with pytest.raises(ValueError, match="earlier than"):
        TimeWindow.parse(END, START)
    with pytest.raises(ValueError, match="earlier than"):
        TimeWindow.parse(START, START)


def test_window_rejects_an_unparseable_timestamp():
    with pytest.raises(ValueError, match="RFC3339"):
        TimeWindow.parse("last tuesday", END)


def test_request_rejects_a_filter_from_another_view():
    with pytest.raises(ValueError, match="does not apply"):
        request(view="entities", rel_type="calls")


def test_request_rejects_filters_on_summary():
    with pytest.raises(ValueError, match="no filters"):
        request(view="summary", rel_type="calls")


def test_request_rejects_an_unknown_view():
    with pytest.raises(ValueError, match="Invalid view"):
        request(view="paths")


def test_mixed_relationships_are_not_ordered_by_red():
    """Only `calls` carries RED, so a mixed sort would rank the rest last."""
    order = _relationship_order(request())

    assert "error_count" not in order
    assert order.startswith("rel_type")


def test_calls_relationships_are_ordered_by_red():
    order = _relationship_order(request(rel_type="calls"))

    assert order.startswith("error_count DESC")


def test_no_match_guidance_drops_the_id_and_keeps_the_type():
    guidance = _no_match_guidance(request(rel_type="calls", src_id="unknown"))

    next_query = guidance["next_query"]
    assert next_query["view"] == "relationships"
    assert next_query["rel_type"] == "calls"
    assert "src_id" not in next_query
    assert "canonical graph entity ID" in guidance["reason"]
    # the window is required, so a retry without it would not run
    GraphRequest.parse(**next_query)


def test_no_match_guidance_without_an_id_points_at_the_summary():
    guidance = _no_match_guidance(request(rel_type="calls"))

    assert guidance["next_query"]["view"] == "summary"


@pytest.mark.parametrize(
    "errno,expected",
    [(1146, "unavailable"), (1142, "permission_denied"), (1105, "error")],
)
def test_probe_classifies_failures(errno, expected):
    capability = GraphView().negotiate(FakeCursor(errno=errno))

    assert capability.status == expected
    assert capability.available is False


def test_probe_reports_an_incompatible_schema():
    """A view that exists but lacks a column this module reads is not usable."""
    columns = {
        **FULL_VIEWS,
        graph.ENTITIES_VIEW: FULL_ENTITY_COLUMNS - {"entity_id_attrs"},
    }

    capability = GraphView().negotiate(FakeCursor(columns=columns))

    assert capability.status == "incompatible_schema"
    assert "entity_id_attrs" in capability.detail


def test_probe_rejects_a_view_it_cannot_read():
    """DESC answers from the catalog, so it does not prove SELECT is allowed."""
    cursor = FakeCursor(columns=FULL_VIEWS, errno=1142, fail_on="SELECT COUNT(*)")

    capability = GraphView().negotiate(cursor)

    assert capability.status == "permission_denied"


def test_probe_caches_a_conclusive_answer_only():
    conclusive = FakeCursor(errno=1146)
    inconclusive = FakeCursor(errno=1105)

    view_a, view_b = GraphView(), GraphView()
    view_a.negotiate(conclusive)
    view_a.negotiate(conclusive)
    view_b.negotiate(inconclusive)
    view_b.negotiate(inconclusive)

    assert len(conclusive.queries) == 1
    assert len(inconclusive.queries) == 2


def test_summary_reports_endpoint_pairs_not_two_sets():
    """Two sets would imply service->node from service->pod and pod->node."""
    cursor = FakeCursor(
        rows=[
            ("runs_on", "service", "k8s.pod", 3),
            ("runs_on", "k8s.pod", "k8s.node", 2),
            ("calls", "service", "service", 14),
        ]
    )

    types = GraphView()._relationship_types(cursor, TimeWindow.parse(START, END))

    runs_on = next(t for t in types if t["type"] == "runs_on")
    assert runs_on["endpoints"] == [
        {"source": "service", "destination": "k8s.pod", "count": 3},
        {"source": "k8s.pod", "destination": "k8s.node", "count": 2},
    ]
    assert runs_on["count"] == 5


def test_identifier_shaped_strings_survive_the_row_decode():
    """Decoding every string would make entity_id "123" a number."""
    columns = ["entity_id", "entity_id_attrs", "source_tables"]
    row = ("123", '{"host":"123"}', '["public.t"]')

    decoded = graph._row_dict(columns, row)

    assert decoded["entity_id"] == "123"
    assert decoded["entity_id_attrs"] == {"host": "123"}
    assert decoded["source_tables"] == ["public.t"]
    assert graph._row_dict(["src_id"], ("null",))["src_id"] == "null"
    assert graph._row_dict(["dst_id"], ("true",))["dst_id"] == "true"


def test_window_params_carry_their_offset():
    """A literal without one is read in the session time zone."""
    params = TimeWindow.parse(START, END).params

    assert all(p.endswith("+00:00") for p in params)


def test_confidence_is_grouped_not_aggregated():
    """A paired bucket and a client-only bucket measure different populations."""
    cursor = FakeCursor(rows=[])
    GraphView().relationships(cursor, request())

    assert "MAX(confidence)" not in cursor.queries[0]
    assert (
        "GROUP BY src_type, src_id, dst_type, dst_id, rel_type, provenance, confidence"
        in (cursor.queries[0])
    )


def test_a_matching_column_name_hides_the_whole_field():
    """The same rule execute_sql applies to a column applies to a field."""
    item = {"entity_type": "service", "descriptive": {"team": "payments"}}

    masked = graph._mask_item(item, graph.mask_patterns(True, ["descriptive"]))

    assert masked["descriptive"] == "******"
    assert masked["entity_type"] == "service"


def test_endpoint_ids_are_hidden_when_the_pattern_names_them():
    """relationships has no attribute names, so the column rule is all it has."""
    item = {"src_id": "checkout", "dst_id": "payment", "rel_type": "calls"}

    masked = graph._mask_item(item, graph.mask_patterns(True, ["src_id"]))

    assert masked["src_id"] == "******"
    assert masked["dst_id"] == "payment"


def test_sensitive_attributes_are_masked_by_name():
    """The column-name rule reaches inside attribute maps too."""
    item = {
        "entity_type": "service",
        "entity_id": "checkout,hunter2",
        "entity_id_attrs": {"service_name": "checkout", "api_key": "sk-live"},
        "descriptive": {"team": "payments", "access_token": "t-123"},
    }

    masked = graph._mask_item(item, graph.mask_patterns(True, None))

    assert masked["entity_id_attrs"] == {
        "service_name": "checkout",
        "api_key": "******",
    }
    assert masked["descriptive"] == {"team": "payments", "access_token": "******"}
    # the id is those values joined, so publishing it would undo the masking
    assert masked["entity_id"] == "******"


def test_adding_a_pattern_never_exposes_what_a_narrower_one_hid():
    """Masking must be monotonic: more patterns can only hide more.

    The id is decided from the original row, so hiding the whole attribute map
    does not stop it from being recognised as the source of the id.
    """
    item = {
        "entity_id": "checkout,sk-live",
        "entity_id_attrs": {"service_name": "checkout", "api_key": "sk-live"},
    }

    exposed = set()
    for extra in (None, ["entity_id_attrs"], ["entity_id"], ["service_name"]):
        masked = graph._mask_item(item, graph.mask_patterns(True, extra))
        assert masked["entity_id"] == "******", extra
        exposed.add(json.dumps(masked, sort_keys=True).count("sk-live"))

    assert exposed == {0}


def test_masking_off_returns_attributes_untouched():
    item = {"entity_id": "checkout", "entity_id_attrs": {"api_key": "sk-live"}}

    assert graph._mask_item(item, graph.mask_patterns(False, ["api_key"])) == item


def test_custom_patterns_extend_the_defaults():
    item = {"entity_id": "checkout", "entity_id_attrs": {"internal_ref": "r-1"}}

    masked = graph._mask_item(item, graph.mask_patterns(True, ["internal_ref"]))

    assert masked["entity_id_attrs"]["internal_ref"] == "******"


def test_truncated_result_says_how_to_narrow():
    """There is no cursor, so the caller needs to know what to filter by."""
    guidance = graph._truncation_guidance(request(rel_type="calls"))

    assert "rel_type" not in guidance["narrow_by"]
    assert "src_id" in guidance["narrow_by"]


@pytest.fixture
def app_state():
    """Application state backed by the mocked MySQL connection."""
    server._state = server.AppState(
        db_config={
            "host": "localhost",
            "port": 4002,
            "user": "",
            "password": "",
            "database": "testdb",
            "time_zone": "",
        },
        pool_config={"pool_name": "greptimedb_pool", "pool_size": 5},
        templates={},
        http_base_url="http://localhost:4000",
    )
    yield server._state
    server._state = None


@pytest.mark.asyncio
async def test_a_probe_that_could_not_run_raises(app_state):
    """The same connection failure must not read as a result on one path and
    a failure on another."""
    app_state.semantic_graph = GraphView(
        capability=GraphCapability("error", detail="2013: Lost connection")
    )

    with pytest.raises(ToolError) as excinfo:
        await server.query_semantic_graph(
            view="summary", start_time=START, end_time=END
        )

    assert "2013: Lost connection" in str(excinfo.value)


@pytest.mark.asyncio
async def test_a_conclusive_probe_still_answers(app_state):
    """An absent graph is an answer, not a failure."""
    app_state.semantic_graph = GraphView(
        capability=GraphCapability("unavailable", detail="Table not found")
    )

    payload = json.loads(
        await server.query_semantic_graph(
            view="summary", start_time=START, end_time=END
        )
    )

    assert payload["status"] == "unavailable"
    assert payload["reason"] == "unavailable"


def test_the_startup_probe_is_time_bounded(app_state, monkeypatch):
    """It runs before the server can serve, so it cannot wait indefinitely."""
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        raise Error("refused")

    monkeypatch.setattr(server, "connect", fake_connect)
    server._withdraw_graph_tool_if_unusable(app_state)

    assert captured["connection_timeout"] == graph.PROBE_TIMEOUT_SECONDS
    assert captured["read_timeout"] == graph.PROBE_TIMEOUT_SECONDS
