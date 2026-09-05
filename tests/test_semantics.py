"""Tests for table semantics capability negotiation and concept search."""

import json

import pytest
from mysql.connector import Error

from greptimedb_mcp_server import semantics, server
from greptimedb_mcp_server.semantics import (
    Capability,
    SearchRequest,
    SemanticsView,
    _build_search_sql,
    _rank_candidates,
    guidance,
    matched_terms,
    search_terms,
)

from conftest import SEMANTICS_VIEW_COLUMNS

FULL = Capability("available", columns=frozenset(SEMANTICS_VIEW_COLUMNS))
NO_DECLARATIONS = Capability(
    "available",
    columns=frozenset(set(SEMANTICS_VIEW_COLUMNS) - {"entity_declarations"}),
)


class ProbeCursor:
    """Cursor that fails the capability probe with a chosen MySQL error."""

    def __init__(self, errno):
        self.errno = errno
        self.probes = 0

    def execute(self, query, args=None):
        self.probes += 1
        # conftest replaces mysql.connector with a mock whose Error takes no
        # keyword arguments, so the errno is attached rather than constructed.
        error = Error("probe failed")
        error.errno = self.errno
        raise error

    def fetchall(self):
        return []


def request(query, signal_type=None, limit=semantics.MAX_SEARCH_LIMIT):
    return SearchRequest.parse(query, signal_type, limit)


@pytest.fixture
def app_state():
    """Install application state backed by the mocked MySQL connection."""
    server._state = server.AppState(
        db_config={
            "host": "localhost",
            "port": 4002,
            "user": "testuser",
            "password": "testpassword",
            "database": "testdb",
            "time_zone": "",
        },
        pool_config={"pool_name": "greptimedb_pool", "pool_size": 5},
        templates={},
        http_base_url="http://localhost:4000",
        mask_enabled=False,
    )
    yield server._state
    server._state = None


def test_search_sql_binds_terms_instead_of_inlining_them():
    sql, params = _build_search_sql(FULL, request("memory"), "testdb")

    assert "memory" not in sql
    assert params[0] == "testdb"
    assert params[1:] == ["%memory%"] * 3


def test_search_sql_escapes_like_wildcards():
    """A term containing % or _ must match literally, not as a wildcard."""
    _, params = _build_search_sql(
        FULL, SearchRequest("raw", ["a_b%c"], None, 10), "testdb"
    )

    assert params[1] == "%a\\_b\\%c%"


def test_search_sql_coalesces_nullable_columns():
    """A NULL column would make the whole OR group NULL and drop the row."""
    sql, _ = _build_search_sql(FULL, request("memory"), "testdb")

    assert "LOWER(COALESCE(semantic_options, ''))" in sql
    assert "LOWER(COALESCE(entity_declarations, ''))" in sql


def test_search_sql_omits_columns_the_view_lacks():
    """Selecting a column the view lacks fails the whole statement to plan."""
    sql, params = _build_search_sql(
        NO_DECLARATIONS, request("memory", "metric"), "testdb"
    )

    assert "entity_declarations" not in sql
    assert params == ["testdb", "metric", "%memory%", "%memory%"]


def test_search_terms_splits_underscores_and_drops_stop_words():
    assert search_terms("redis_used_memory for the host") == [
        "redis",
        "used",
        "memory",
        "host",
    ]


def test_search_terms_deduplicates_and_caps():
    terms = search_terms(" ".join(f"term{i}" for i in range(20)) + " term0")

    assert len(terms) == semantics.MAX_SEARCH_TERMS
    assert len(set(terms)) == len(terms)


def test_search_sql_ors_the_terms_together():
    """ANDing terms would require every word and flatten the ranking."""
    sql, params = _build_search_sql(FULL, request("redis memory usage"), "testdb")

    where = sql.split(" WHERE ", 1)[1].split(" ORDER BY", 1)[0]
    schema_filter, term_group = where.split(" AND ", 1)

    assert schema_filter == "table_schema = %s"
    # One OR group per term, ORed with each other, and nothing ANDed inside.
    assert term_group.count("LIKE %s") == 9
    assert " AND " not in term_group
    assert params[1:] == ["%redis%"] * 3 + ["%memory%"] * 3 + ["%usage%"] * 3


def test_search_terms_keeps_io_as_one_token():
    """Split on the slash, `I/O` becomes two one-letter terms and vanishes."""
    assert search_terms("node disk write I/O") == ["node", "disk", "write", "io"]
    assert search_terms("system_io_w_s") == ["system", "io"]
    assert search_terms("CPU of a pod") == ["cpu", "pod"]


def test_matched_terms_expands_io_direction_abbreviations():
    """`system_io_w_s` is a write metric; the query says `write`."""
    assert matched_terms(["write", "io"], "system_io_w_s") == ["write", "io"]
    assert matched_terms(["read", "io"], "system_io_r_s") == ["read", "io"]


def test_matched_terms_does_not_expand_nonadjacent_io_tokens():
    assert matched_terms(["write", "io"], "unrelated_w_metric_io") == ["io"]


def test_matched_terms_requires_whole_token_for_short_terms():
    """`geo` must not match `range of` once punctuation is stripped."""
    assert matched_terms(["geo"], "range of requests") == []
    assert matched_terms(["geo"], "geo service") == ["geo"]


def test_rank_candidates_keeps_field_types_stable():
    """A malformed payload must not turn semantic_options into a string."""
    columns = ["table_name", "semantic_options", "entity_declarations"]

    [malformed] = _rank_candidates(
        columns, [("redis_memory", "not json", None)], ["redis"]
    )
    [empty] = _rank_candidates(columns, [("redis_memory", "{}", None)], ["redis"])

    assert "semantic_options" not in malformed
    assert malformed["raw_options"] == "not json"
    assert empty["semantic_options"] == {}


def test_matched_terms_allows_substring_for_longer_terms():
    assert matched_terms(["memory"], "redis_used_memory_bytes") == ["memory"]


def test_search_request_rejects_an_unusable_query():
    with pytest.raises(ValueError) as excinfo:
        SearchRequest.parse("of the")
    assert "at least one term" in str(excinfo.value)


def test_search_request_rejects_an_unknown_signal_type():
    with pytest.raises(ValueError) as excinfo:
        SearchRequest.parse("memory", "metrics")
    assert "Invalid signal_type" in str(excinfo.value)


@pytest.mark.parametrize(
    "errno,expected",
    [
        (1146, "unavailable"),
        (1142, "permission_denied"),
        (1105, "error"),
    ],
)
def test_capability_probe_classifies_failures(errno, expected):
    capability = SemanticsView().negotiate(ProbeCursor(errno))

    assert capability.status == expected
    assert capability.available is False


def test_capability_probe_does_not_cache_transient_failures():
    """An unclassified error must not disable semantics for the process."""
    view = SemanticsView()
    cursor = ProbeCursor(1105)

    view.negotiate(cursor)
    view.negotiate(cursor)

    assert cursor.probes == 2


def test_capability_probe_caches_a_missing_view():
    view = SemanticsView()
    cursor = ProbeCursor(1146)

    view.negotiate(cursor)
    view.negotiate(cursor)

    assert cursor.probes == 1


def test_guidance_flags_a_version_limit():
    hints = guidance(
        {
            "included": True,
            "available": True,
            "found": True,
            "missing_columns": ["entity_declarations"],
        }
    )

    assert len(hints) == 1
    assert "does not expose entity_declarations" in hints[0]
    assert "not evidence" in hints[0]


def test_guidance_lists_declared_entity_types():
    hints = guidance(
        {
            "included": True,
            "available": True,
            "found": True,
            "entity_declarations": [
                {"entity_type": "service", "id": ["service_name"]},
                {"entity_type": "host", "id": ["host"]},
            ],
        }
    )

    assert "host, service" in hints[0]
    assert any("id_qualifier" in line for line in hints)


def test_guidance_when_the_table_declares_no_entities():
    hints = guidance(
        {"included": True, "available": True, "found": True, "entity_declarations": []}
    )

    assert len(hints) == 1
    assert "declares no semantic entities" in hints[0]


def test_guidance_separates_permission_denied_from_an_absent_view():
    denied = guidance(
        {"included": True, "available": False, "reason": "permission_denied"}
    )
    absent = guidance({"included": True, "available": False, "reason": "unavailable"})

    assert "cannot read it" in denied[0]
    assert "may not support" in absent[0]


@pytest.mark.asyncio
async def test_search_ranks_by_matched_term_count(app_state):
    result = json.loads(await server.search_table_semantics(query="redis used memory"))

    assert result["available"] is True
    assert result["matches"][0]["table"] == "redis_used_memory"
    assert result["matches"][0]["matched_terms"] == ["redis", "used", "memory"]
    assert result["matched_table_count"] == 1


@pytest.mark.asyncio
async def test_search_reports_columns_the_view_lacks(app_state):
    """An older view searches less, and the result has to say so."""
    app_state.table_semantics = SemanticsView(capability=NO_DECLARATIONS)

    result = json.loads(await server.search_table_semantics(query="redis memory"))

    assert result["searched_columns"] == ["table_name", "semantic_options"]
    assert result["unsearched_columns"] == ["entity_declarations"]


@pytest.mark.asyncio
async def test_search_reports_an_unavailable_view(app_state):
    app_state.table_semantics = SemanticsView(
        capability=Capability("unavailable", detail="Table not found")
    )

    result = json.loads(await server.search_table_semantics(query="memory"))

    assert result["available"] is False
    assert result["reason"] == "unavailable"
    assert result["matches"] == []
