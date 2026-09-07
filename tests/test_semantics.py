"""Tests for table semantics capability negotiation and concept search."""

import json

import pytest
from mysql.connector import Error

from greptimedb_mcp_server import semantics, server
from greptimedb_mcp_server.semantics import (
    Capability,
    _tokens,
    SearchRequest,
    SemanticsView,
    _build_search_sql,
    _rank_candidates,
    guidance,
    _matched_terms,
    _search_terms,
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


def _where(sql):
    return sql.split(" WHERE ", 1)[1].split(" ORDER BY", 1)[0]


def test_search_sql_binds_terms_instead_of_inlining_them():
    sql, params = _build_search_sql(FULL, request("memory"), "testdb")

    assert "memory" not in sql
    # once for the relevance score, once for the filter, over three columns
    assert params.count("%memory%") == 6
    assert "testdb" in params


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
    # two searchable columns left, scored once and filtered once
    assert params.count("%memory%") == 4
    assert params.count("metric") == 1


def test_search_terms_drops_stop_words_and_single_characters():
    assert _search_terms("cpu of a pod") == ["cpu", "pod"]


def test_search_terms_deduplicates_and_caps():
    terms = _search_terms(" ".join(f"term{i}" for i in range(20)) + " term0")

    assert len(terms) == semantics.MAX_SEARCH_TERMS
    assert len(set(terms)) == len(terms)


def test_search_sql_ors_the_terms_together():
    """ANDing terms would require every word and flatten the ranking."""
    sql, params = _build_search_sql(FULL, request("redis memory usage"), "testdb")

    schema_filter, term_group = _where(sql).split(" AND ", 1)

    assert schema_filter == "table_schema = %s"
    # One OR group per term, ORed with each other, and nothing ANDed inside.
    assert term_group.count("LIKE %s") == 9
    assert " AND " not in term_group
    for term in ("%redis%", "%memory%", "%usage%"):
        assert params.count(term) == 6


def test_scan_is_ordered_by_relevance_not_by_name():
    """Ordering by name would cut the candidate set alphabetically, so a table
    hitting every term can lose to one hitting a single term and never reach
    ranking at all."""
    sql, _ = _build_search_sql(FULL, request("redis used memory"), "testdb")

    assert "ORDER BY term_hits DESC, table_name" in sql
    assert sql.count("CASE WHEN") == 3


def test_search_terms_keeps_io_as_one_token():
    """Split on the slash, `I/O` becomes two one-letter terms and vanishes."""
    assert _search_terms("node disk write I/O") == ["node", "disk", "write", "io"]
    assert _search_terms("system_io_w_s") == ["system", "io"]


def test_matched_terms_expands_io_direction_abbreviations():
    """`system_io_w_s` is a write metric; the query says `write`."""
    assert _matched_terms(["write", "io"], "system_io_w_s") == ["write", "io"]
    assert _matched_terms(["read", "io"], "system_io_r_s") == ["read", "io"]


def test_matched_terms_does_not_expand_nonadjacent_io_tokens():
    assert _matched_terms(["write", "io"], "unrelated_w_metric_io") == ["io"]


def test_tokens_split_identifiers_however_they_are_written():
    """snake, camel, acronym, dotted and numeric all reduce to the same words."""
    assert _tokens("used_memory_bytes") == ["used", "memory", "bytes"]
    assert _tokens("usedMemoryBytes") == ["used", "memory", "bytes"]
    assert _tokens("nodeCPUSeconds") == ["node", "cpu", "seconds"]
    assert _tokens("os.linux.mem-free") == ["os", "linux", "mem", "free"]
    assert _tokens("redis_Redis_6379_Redis___used_memory_") == [
        "redis",
        "redis",
        "6379",
        "redis",
        "used",
        "memory",
    ]


def test_camel_case_names_are_searchable():
    """This worked before only as a side effect of substring matching."""
    assert _matched_terms(["memory"], "usedMemoryBytes") == ["memory"]
    assert _matched_terms(["cpu"], "nodeCPUSeconds") == ["cpu"]


def test_a_term_matches_a_token_prefix():
    assert _matched_terms(["mem"], "os_linux_memory_usage_percent") == ["mem"]
    assert _matched_terms(["dur"], "http_request_duration_seconds") == ["dur"]


def test_prefix_matching_does_not_cross_token_boundaries():
    """A substring test let `geo` match `rangeof`; a prefix per token cannot."""
    assert _matched_terms(["geo"], "range_of_requests") == []
    assert _matched_terms(["geo"], "rangeof_requests") == []


def test_search_recalls_the_rows_a_synonym_would_match():
    """Nothing in `system_io_w_s` contains `write`, so the scan must ask for io."""
    _, params = _build_search_sql(FULL, request("write"), "testdb")

    assert "%io%" in params


def test_matched_terms_requires_whole_token_for_short_terms():
    """`geo` must not match `range of` once punctuation is stripped."""
    assert _matched_terms(["geo"], "range of requests") == []
    assert _matched_terms(["geo"], "geo service") == ["geo"]


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


def _declared(**declaration):
    return {
        "included": True,
        "available": True,
        "found": True,
        "entity_declarations": [declaration],
    }


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


def test_guidance_warns_only_when_a_qualifier_can_be_dropped():
    """Only a hand-written declaration can drop the convention's qualifier."""
    at_risk = guidance(_declared(entity_type="service", id=["name"], origin="declared"))
    qualified = guidance(
        _declared(
            entity_type="service",
            id=["name"],
            origin="declared",
            id_qualifier="namespace",
        )
    )
    conventional = guidance(
        _declared(entity_type="service", id=["name"], origin="convention")
    )

    assert any("id_qualifier" in line for line in at_risk)
    assert not any("id_qualifier" in line for line in qualified)
    assert not any("id_qualifier" in line for line in conventional)


def test_guidance_reports_a_failed_read_as_a_failed_read():
    """A transient query failure is not a statement about the server version."""
    hints = guidance(
        {"included": True, "available": False, "reason": "error", "error": "timeout"}
    )

    assert "may not support" not in hints[0]
    assert "retry" in hints[0]


def test_ranking_ignores_the_schemas_own_key_names():
    """Structural words would otherwise score every table alike."""
    columns = ["table_name", "semantic_options", "entity_declarations"]
    rows = [
        ("it_cpu", '{"metric.type":"gauge","metric.unit":"percent"}', None),
        ("orders_total", '{"metric.type":"counter"}', None),
    ]

    assert _rank_candidates(columns, rows, ["metric", "type", "unit"]) == []
    # Values still rank, so the vocabulary a user actually searches survives.
    [gauge] = _rank_candidates(columns, rows, ["gauge"])
    assert gauge["table"] == "it_cpu"


def test_ranking_still_sees_entity_declaration_values():
    columns = ["table_name", "semantic_options", "entity_declarations"]
    rows = [("t", None, '[{"entity_type":"k8s.pod","id":["pod"]}]')]

    [match] = _rank_candidates(columns, rows, ["pod"])

    assert match["matched_terms"] == ["pod"]


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
async def test_every_search_outcome_shares_one_shape(app_state):
    """A caller parses one schema whether the search worked or not."""
    common = {
        "query",
        "terms",
        "signal_type",
        "available",
        "matched_table_count",
        "matches",
    }

    ok = json.loads(await server.search_table_semantics(query="redis memory"))

    app_state.table_semantics = SemanticsView(
        capability=Capability("unavailable", detail="Table not found")
    )
    unavailable = json.loads(await server.search_table_semantics(query="redis memory"))

    failed = semantics.search_failure(
        SearchRequest.parse("redis memory"), "error", "connection lost"
    )

    for payload in (ok, unavailable, failed):
        assert common <= set(payload), sorted(common - set(payload))
    assert ok["available"] is True
    assert unavailable["available"] is False
    assert unavailable["reason"] == "unavailable"
    assert failed["available"] is False


@pytest.mark.asyncio
async def test_search_reports_an_unavailable_view(app_state):
    app_state.table_semantics = SemanticsView(
        capability=Capability("unavailable", detail="Table not found")
    )

    result = json.loads(await server.search_table_semantics(query="memory"))

    assert result["available"] is False
    assert result["reason"] == "unavailable"
    assert result["matches"] == []
