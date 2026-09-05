"""Tests for the semantic graph window, ordering, and capability contracts."""

from datetime import datetime, timezone

import pytest
from mysql.connector import Error

from greptimedb_mcp_server import graph
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
FULL_RELATIONSHIP_COLUMNS = frozenset(
    graph.RELATIONSHIP_REQUIRED_COLUMNS | set(graph.RED_COLUMNS) | {"duration_max"}
)


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


def test_window_treats_a_naive_timestamp_as_utc():
    window = TimeWindow.parse("2026-09-05T07:00:00", "2026-09-05T08:00:00")

    assert window.start == datetime(2026, 9, 5, 7, tzinfo=timezone.utc)
    assert window.describe()["bounds"] == "[start, end)"


def test_window_normalizes_an_offset_to_utc():
    window = TimeWindow.parse("2026-09-05T09:00:00+02:00", END)

    assert window.start == datetime(2026, 9, 5, 7, tzinfo=timezone.utc)


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

    assert guidance["next_query"] == {"view": "relationships", "rel_type": "calls"}
    assert "canonical graph entity ID" in guidance["reason"]


def test_no_match_guidance_without_an_id_points_at_the_summary():
    guidance = _no_match_guidance(request(rel_type="calls"))

    assert guidance["next_query"] == {"view": "summary"}


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
        graph.ENTITIES_VIEW: FULL_ENTITY_COLUMNS - {"entity_id_attrs"},
        graph.RELATIONSHIPS_VIEW: FULL_RELATIONSHIP_COLUMNS,
    }

    capability = GraphView().negotiate(FakeCursor(columns=columns))

    assert capability.status == "incompatible_schema"
    assert "entity_id_attrs" in capability.detail


def test_probe_rejects_a_view_it_cannot_read():
    """DESC answers from the catalog, so it does not prove SELECT is allowed."""
    columns = {
        graph.ENTITIES_VIEW: FULL_ENTITY_COLUMNS,
        graph.RELATIONSHIPS_VIEW: FULL_RELATIONSHIP_COLUMNS,
    }
    cursor = FakeCursor(columns=columns, errno=1142, fail_on="SELECT COUNT(*)")

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


def test_summary_groups_endpoint_types_by_relationship():
    """The caller learns the graph's shape without reading its edges."""
    cursor = FakeCursor(
        rows=[
            ("runs_on", "service", "k8s.pod", 3),
            ("runs_on", "k8s.pod", "k8s.node", 2),
            ("calls", "service", "service", 14),
        ]
    )

    types = GraphView()._relationship_types(cursor, TimeWindow.parse(START, END))

    runs_on = next(t for t in types if t["type"] == "runs_on")
    assert runs_on["source_types"] == ["service", "k8s.pod"]
    assert runs_on["destination_types"] == ["k8s.pod", "k8s.node"]
    assert runs_on["count"] == 5


def test_relationships_omit_red_columns_the_view_lacks():
    """Selecting a column the view lacks fails the statement to plan."""
    view = GraphView(
        capability=GraphCapability(
            "available",
            entity_columns=FULL_ENTITY_COLUMNS,
            relationship_columns=graph.RELATIONSHIP_REQUIRED_COLUMNS,
        )
    )
    cursor = FakeCursor(rows=[])

    view.relationships(cursor, request())

    assert "request_count" not in cursor.queries[0]
    assert "duration_max" not in cursor.queries[0]
