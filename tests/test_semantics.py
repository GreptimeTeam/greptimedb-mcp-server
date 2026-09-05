"""Tests for table semantics capability negotiation and concept search."""

import json

import pytest
from mysql.connector import Error

from greptimedb_mcp_server import server
from greptimedb_mcp_server.server import (
    SEMANTICS_COLUMNS,
    SemanticsCapability,
    _entity_declaration_guidance,
    _matched_terms,
    _search_table_semantics_sql,
    _search_terms,
    _table_semantics_capability,
    search_table_semantics,
)

from conftest import SEMANTICS_VIEW_COLUMNS

FULL = SemanticsCapability("available", columns=frozenset(SEMANTICS_VIEW_COLUMNS))
NO_DECLARATIONS = SemanticsCapability(
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


def _state():
    return server.AppState(
        db_config={"database": "testdb"},
        pool_config={},
        templates={},
        http_base_url="http://localhost:4000",
    )


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
    sql, params = _search_table_semantics_sql(FULL, ["memory"], "testdb", None)

    assert "memory" not in sql
    assert params[0] == "testdb"
    assert params[1:] == ["%memory%"] * 3


def test_search_sql_escapes_like_wildcards():
    """A term containing % or _ must match literally, not as a wildcard."""
    _, params = _search_table_semantics_sql(FULL, ["a_b%c"], "testdb", None)

    assert params[1] == "%a\\_b\\%c%"


def test_search_sql_coalesces_nullable_columns():
    """A NULL column would make the whole OR group NULL and drop the row."""
    sql, _ = _search_table_semantics_sql(FULL, ["memory"], "testdb", None)

    assert "LOWER(COALESCE(semantic_options, ''))" in sql
    assert "LOWER(COALESCE(entity_declarations, ''))" in sql


def test_search_sql_omits_columns_the_view_lacks():
    """Selecting a column the view lacks fails the whole statement to plan."""
    sql, params = _search_table_semantics_sql(
        NO_DECLARATIONS, ["memory"], "testdb", "metric"
    )

    assert "entity_declarations" not in sql
    assert params == ["testdb", "metric", "%memory%", "%memory%"]


def test_search_sql_filters_by_signal_type():
    sql, params = _search_table_semantics_sql(FULL, ["memory"], "testdb", "log")

    assert "signal_type = %s" in sql
    assert "log" in params


def test_search_terms_splits_underscores_and_drops_stop_words():
    assert _search_terms("redis_used_memory for the host") == [
        "redis",
        "used",
        "memory",
        "host",
    ]


def test_search_terms_deduplicates_and_caps():
    terms = _search_terms(" ".join(f"term{i}" for i in range(20)) + " term0")

    assert len(terms) == server.MAX_SEARCH_TERMS
    assert len(set(terms)) == len(terms)


def test_matched_terms_requires_whole_token_for_short_terms():
    """`geo` must not match `range of` once punctuation is stripped."""
    assert _matched_terms(["geo"], "range of requests") == []
    assert _matched_terms(["geo"], "geo service") == ["geo"]


def test_matched_terms_allows_substring_for_longer_terms():
    assert _matched_terms(["memory"], "redis_used_memory_bytes") == ["memory"]


@pytest.mark.parametrize(
    "errno,expected",
    [
        (1146, "unavailable"),
        (1142, "permission_denied"),
        (1105, "error"),
    ],
)
def test_capability_probe_classifies_failures(errno, expected):
    state = _state()

    capability = _table_semantics_capability(state, ProbeCursor(errno))

    assert capability.status == expected
    assert capability.available is False


def test_capability_probe_does_not_cache_transient_failures():
    """An unclassified error must not disable semantics for the process."""
    state = _state()
    cursor = ProbeCursor(1105)

    _table_semantics_capability(state, cursor)
    _table_semantics_capability(state, cursor)

    assert state.semantics_capability is None
    assert cursor.probes == 2


def test_capability_probe_caches_a_missing_view():
    state = _state()
    cursor = ProbeCursor(1146)

    _table_semantics_capability(state, cursor)
    _table_semantics_capability(state, cursor)

    assert state.semantics_capability.status == "unavailable"
    assert cursor.probes == 1


def test_entity_declaration_guidance_flags_a_version_limit():
    guidance = _entity_declaration_guidance(
        {
            "included": True,
            "found": True,
            "missing_columns": ["entity_declarations"],
        }
    )

    assert len(guidance) == 1
    assert "does not expose entity_declarations" in guidance[0]
    assert "not evidence" in guidance[0]


def test_entity_declaration_guidance_lists_declared_types():
    guidance = _entity_declaration_guidance(
        {
            "included": True,
            "found": True,
            "entity_declarations": [
                {"entity_type": "service", "id": ["service_name"]},
                {"entity_type": "host", "id": ["host"]},
            ],
        }
    )

    assert "host, service" in guidance[0]
    assert any("id_qualifier" in line for line in guidance)


def test_entity_declaration_guidance_when_table_declares_none():
    guidance = _entity_declaration_guidance(
        {"included": True, "found": True, "entity_declarations": []}
    )

    assert len(guidance) == 1
    assert "declares no semantic entities" in guidance[0]


@pytest.mark.asyncio
async def test_search_ranks_by_matched_term_count(app_state):
    result = json.loads(await search_table_semantics(query="redis used memory"))

    assert result["available"] is True
    assert result["matches"][0]["table"] == "redis_used_memory"
    assert result["matches"][0]["matched_terms"] == ["redis", "used", "memory"]
    assert result["matched_table_count"] == 1


@pytest.mark.asyncio
async def test_search_reports_columns_the_view_lacks(app_state):
    """An older view searches less, and the result has to say so."""
    app_state.semantics_capability = NO_DECLARATIONS

    result = json.loads(await search_table_semantics(query="redis memory"))

    assert result["searched_columns"] == ["table_name", "semantic_options"]
    assert result["unsearched_columns"] == ["entity_declarations"]


@pytest.mark.asyncio
async def test_search_rejects_an_unusable_query(app_state):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await search_table_semantics(query="of the")
    assert "at least one term" in str(excinfo.value)


@pytest.mark.asyncio
async def test_search_rejects_an_unknown_signal_type(app_state):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await search_table_semantics(query="memory", signal_type="metrics")
    assert "Invalid signal_type" in str(excinfo.value)


@pytest.mark.asyncio
async def test_search_reports_an_unavailable_view(app_state):
    server._state.semantics_capability = SemanticsCapability(
        "unavailable", detail="Table not found"
    )

    result = json.loads(await search_table_semantics(query="memory"))

    assert result["available"] is False
    assert result["reason"] == "unavailable"
    assert result["matches"] == []


def test_semantics_columns_match_the_documented_view():
    """The SELECT list is the 1.3 view; drift here silently drops a field."""
    assert SEMANTICS_COLUMNS == SEMANTICS_VIEW_COLUMNS
