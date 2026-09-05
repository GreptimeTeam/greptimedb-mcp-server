"""Reads `information_schema.table_semantics`.

The view's shape varies by server version -- `entity_declarations` arrived in
GreptimeDB 1.3 -- and selecting a column it lacks fails the whole statement to
plan, so `SemanticsView` negotiates the column list once and builds every query
from what is actually exposed.
"""

import json
import re
from dataclasses import dataclass

from mysql.connector import Error

VIEW = "information_schema.table_semantics"

COLUMNS = (
    "table_catalog",
    "table_schema",
    "table_name",
    "table_id",
    "signal_type",
    "source",
    "source_version",
    "pipeline",
    "metadata_quality",
    "semantic_options",
    "entity_declarations",
)

SEARCH_COLUMNS = ("table_name", "semantic_options", "entity_declarations")

VALID_SIGNAL_TYPES = ("metric", "log", "trace", "event")

MAX_SEARCH_LIMIT = 50
MAX_SEARCH_TERMS = 10
# Rows read before ranking. Ranking needs the whole candidate set, so this caps
# the read rather than the reported matches.
SEARCH_SCAN_LIMIT = 1000

STOP_WORDS = frozenset(
    {"and", "for", "from", "in", "of", "on", "or", "the", "to", "with"}
)

# Tells a missing view from a rejected one. Anything else stays uncached, so a
# transient failure does not disable the feature for the life of the process.
ERRNO_TABLE_NOT_FOUND = 1146
ERRNO_PERMISSION_DENIED = frozenset({1044, 1045, 1142, 1143, 1227})


@dataclass(frozen=True)
class Capability:
    """What the connected GreptimeDB exposes of the semantics view."""

    status: str
    columns: frozenset[str] = frozenset()
    detail: str | None = None

    @property
    def available(self) -> bool:
        return self.status == "available"

    @property
    def cacheable(self) -> bool:
        """Whether the probe answered, as opposed to failing to run."""
        return self.status in ("available", "unavailable", "permission_denied")

    def selectable(self, columns) -> list[str]:
        return [column for column in columns if column in self.columns]

    def missing(self, columns) -> list[str]:
        return [column for column in columns if column not in self.columns]


@dataclass(frozen=True)
class SearchRequest:
    """A validated concept search."""

    query: str
    terms: list[str]
    signal_type: str | None
    limit: int

    @classmethod
    def parse(
        cls, query: str, signal_type: str | None = None, limit: int = MAX_SEARCH_LIMIT
    ) -> "SearchRequest":
        """Validate tool arguments before a connection is taken from the pool.

        Raises ValueError, which the tool boundary turns into a ToolError.
        """
        if not query or not query.strip():
            raise ValueError("query is required")
        if signal_type is not None and signal_type not in VALID_SIGNAL_TYPES:
            raise ValueError(
                f"Invalid signal_type: {signal_type}. "
                f"Must be one of: {', '.join(VALID_SIGNAL_TYPES)}"
            )
        terms = _search_terms(query)
        if not terms:
            raise ValueError(
                "query must contain at least one term of two or more characters"
            )
        return cls(
            query=query,
            terms=terms,
            signal_type=signal_type,
            limit=max(1, min(limit, MAX_SEARCH_LIMIT)),
        )


def _join_slash_abbreviations(value: str) -> str:
    """Fold `I/O` into `io` so it survives tokenizing as one term.

    Split on the slash it becomes `i` and `o`, which the one-character filter
    then discards, and a search for `I/O` has nothing left to look for.
    """
    return re.sub(r"\b([A-Za-z])\s*/\s*([A-Za-z])\b", r"\1\2", value)


def _search_terms(query: str) -> list[str]:
    """Split a concept query into distinct searchable terms.

    Underscores become separators so that a query for `used memory` still
    matches `redis___used_memory_`. The character class also keeps LIKE
    metacharacters out of a term, which is what lets a term go into a LIKE
    pattern as-is.
    """
    normalized = _join_slash_abbreviations(query).lower().replace("_", " ")
    terms = (
        term
        for term in re.findall(r"[a-z0-9.:-]+", normalized)
        if len(term) > 1 and term not in STOP_WORDS
    )
    return list(dict.fromkeys(terms))[:MAX_SEARCH_TERMS]


def _matched_terms(terms: list[str], searchable: str) -> list[str]:
    """Return the terms a candidate matched, for ranking.

    Terms of one or two characters must match a whole token: a substring test
    would let `geo` match `range of`.

    Legacy metric names abbreviate the I/O direction as a single letter, so
    adjacent `io_w` and `io_r` are read back as `write` and `read`;
    `unrelated_w_metric_io` is not a write metric. This and
    `_join_slash_abbreviations` are ported from GreptimeTeam/agent-rca-bench,
    where they are what made concept search work on legacy metric schemas.
    """
    normalized = _join_slash_abbreviations(searchable).lower()
    token_list = re.findall(r"[a-z0-9]+", normalized)
    tokens = set(token_list)
    adjacent = set(zip(token_list, token_list[1:]))
    for direction, word in (("w", "write"), ("r", "read")):
        if ("io", direction) in adjacent:
            tokens.add(word)
    return [
        term
        for term in terms
        if term in tokens or (len(term) > 2 and term in normalized)
    ]


def _parse_json_column(value, column: str, expected: type):
    """Decode a semantics JSON column, keeping the raw text when it is not usable.

    Returns (parsed, raw, error): callers surface raw/error instead of dropping
    a payload whose shape this version does not expect.
    """
    if not value:
        return None, None, None
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as e:
        return None, value, str(e)
    if not isinstance(parsed, expected):
        shape = "object" if expected is dict else "array"
        return None, value, f"{column} is not a JSON {shape}"
    return parsed, None, None


def guidance(profile: dict) -> list[str]:
    """Query hints derived from a table's semantic profile."""
    if not profile.get("included"):
        return []

    hints = _availability_guidance(profile)
    hints.extend(_entity_declaration_guidance(profile))
    hints.extend(_signal_guidance(profile))
    return hints


def _availability_guidance(profile: dict) -> list[str]:
    if not profile.get("available", True):
        if profile.get("reason") == "permission_denied":
            return [
                f"{VIEW} exists but this account cannot read it. Signal type "
                "and query pattern below are schema/sample-based inference."
            ]
        return [
            "Table semantic metadata is unavailable. The connected GreptimeDB "
            f"version may not support {VIEW}."
        ]
    if not profile.get("found"):
        return [
            "No table semantic metadata was found. Treat signal type and "
            "query pattern as schema/sample-based inference."
        ]
    return []


def _entity_declaration_guidance(profile: dict) -> list[str]:
    """Explain what the table contributes to the semantic graph, if anything."""
    if not profile.get("found"):
        return []

    if "entity_declarations" in (profile.get("missing_columns") or []):
        return [
            "This GreptimeDB version does not expose entity_declarations, so "
            "the entities this table contributes to the semantic graph cannot "
            "be read here. Absence is a version limit, not evidence that the "
            "table declares none."
        ]

    declarations = profile.get("entity_declarations")
    if not declarations:
        return [
            "This table declares no semantic entities, so it contributes no "
            "nodes to the semantic graph. Its rows are still queryable."
        ]

    types = sorted(
        {
            str(item.get("entity_type"))
            for item in declarations
            if isinstance(item, dict) and item.get("entity_type")
        }
    )
    hints = []
    if types:
        hints.append(
            f"This table contributes these semantic entities: {', '.join(types)}. "
            "Each declaration's id lists the identifying columns, in order."
        )
    # The declared/convention split and a dropped id_qualifier are the only
    # observable causes of one process appearing under two entity ids.
    hints.append(
        "Check origin and id_qualifier on each declaration before treating two "
        "ids as two things. An explicit declaration replaces the built-in "
        "convention for that entity type outright, so one that omits "
        "id_qualifier silently drops the qualifier the convention would have "
        "applied, and the same process can then appear under two ids."
    )
    return hints


def _signal_guidance(profile: dict) -> list[str]:
    signal_type = profile.get("signal_type")
    options = profile.get("options") or {}
    metric_type = options.get("metric.type")

    if signal_type == "trace":
        return [
            "This table represents traces. Prefer latency, error span, and "
            "service-level aggregation queries."
        ]
    if signal_type == "log":
        return [
            "This table represents logs. Prefer full-text search plus "
            "severity, time, and service aggregations."
        ]
    if signal_type != "metric":
        return []

    hints = []
    if metric_type == "counter":
        hints.append(
            "This table is a counter metric. Prefer rate or increase queries "
            "for trend analysis."
        )
    elif metric_type == "gauge":
        hints.append(
            "This table is a gauge metric. Prefer absolute value, avg, min, "
            "max, or percentile analysis."
        )
    elif metric_type == "histogram":
        hints.append(
            "This table is a histogram metric. Prefer bucket/count/sum based "
            "percentile analysis."
        )
    if profile.get("metadata_quality") == "inferred":
        hints.append(
            "Metric type was inferred from naming. Re-check the query choice "
            "if the metric name is non-standard."
        )
    return hints


def _build_search_sql(
    capability: Capability, request: SearchRequest, table_schema: str
) -> tuple[str, list]:
    """Build the candidate query and its bound parameters."""
    columns = capability.selectable(COLUMNS)
    searchable = capability.selectable(SEARCH_COLUMNS)

    predicates = ["table_schema = %s"]
    params: list = [table_schema]
    if request.signal_type:
        predicates.append("signal_type = %s")
        params.append(request.signal_type)

    # Terms are ORed with each other: a table matching some of them is a
    # candidate, and how many it matched is what ranking is for. ANDing them
    # would drop `redis_used_memory` from a search for "redis memory usage"
    # and leave every surviving row with an identical score.
    term_clauses = []
    for term in request.terms:
        pattern = f"%{term}%"
        # COALESCE keeps a NULL column from making the whole OR group NULL,
        # which would drop rows that matched on another column.
        clauses = [f"LOWER(COALESCE({column}, '')) LIKE %s" for column in searchable]
        term_clauses.append(f"({' OR '.join(clauses)})")
        params.extend([pattern] * len(clauses))
    predicates.append(f"({' OR '.join(term_clauses)})")

    sql = (
        f"SELECT {', '.join(columns)} FROM {VIEW} "
        f"WHERE {' AND '.join(predicates)} "
        f"ORDER BY table_name LIMIT {SEARCH_SCAN_LIMIT}"
    )
    return sql, params


def _rank_candidates(columns: list[str], rows: list, terms: list[str]) -> list[dict]:
    """Score rows by matched terms, dropping rows no term actually matched."""
    candidates = []
    for row in rows:
        values = dict(zip(columns, row))
        candidate = _candidate(values, terms)
        if candidate is not None:
            candidates.append(candidate)

    candidates.sort(
        key=lambda item: (-len(item["matched_terms"]), str(item["table"]).lower())
    )
    return candidates


def _candidate(values: dict, terms: list[str]) -> dict | None:
    searchable = " ".join(
        str(values.get(column)) for column in SEARCH_COLUMNS if values.get(column)
    )
    matched = _matched_terms(terms, searchable)
    if not matched:
        # The SQL LIKE matched a substring the ranking rules reject, such as a
        # short term inside a longer word.
        return None

    candidate = {
        "table": values.get("table_name"),
        "signal_type": values.get("signal_type"),
        "source": values.get("source"),
        "source_version": values.get("source_version"),
        "pipeline": values.get("pipeline"),
        "metadata_quality": values.get("metadata_quality"),
        "matched_terms": matched,
    }
    for column, expected, raw_key in (
        ("semantic_options", dict, "raw_options"),
        ("entity_declarations", list, "raw_entity_declarations"),
    ):
        parsed, raw, _ = _parse_json_column(values.get(column), column, expected)
        if parsed is not None:
            candidate[column] = parsed
        elif raw is not None:
            candidate[raw_key] = raw
    return candidate


@dataclass
class SemanticsView:
    """A handle on the semantics view that remembers what it exposes."""

    capability: Capability | None = None

    def negotiate(self, cursor) -> Capability:
        """Probe the view once per process and reuse the answer.

        Racing callers may both probe; the result is identical, so the extra
        DESC is cheaper than serializing every read on a lock.
        """
        if self.capability is not None:
            return self.capability
        capability = _probe(cursor)
        if capability.cacheable:
            self.capability = capability
        return capability

    def fetch(self, cursor, table_schema: str, table_name: str) -> dict:
        """Read one table's semantic profile, degrading if a column is absent."""
        capability = self.negotiate(cursor)
        if not capability.available:
            return _unavailable(capability.status, capability.detail)

        columns = capability.selectable(COLUMNS)
        try:
            cursor.execute(
                f"SELECT {', '.join(columns)} FROM {VIEW} "
                "WHERE table_schema = %s AND table_name = %s",
                (table_schema, table_name),
            )
            # fetchall() drains the unbuffered cursor before the next query
            # runs; the WHERE clause matches at most one row.
            rows = cursor.fetchall()
        except Error as e:
            return _unavailable("error", str(e))

        profile = {
            "included": True,
            "available": True,
            "found": bool(rows),
        }
        missing = capability.missing(COLUMNS)
        if missing:
            profile["missing_columns"] = missing
        if rows:
            profile.update(_row_profile(dict(zip(columns, rows[0]))))
        return profile

    def search(self, cursor, table_schema: str, request: SearchRequest) -> dict:
        """Rank tables in one schema by how many query terms they matched."""
        capability = self.negotiate(cursor)
        if not capability.available:
            return {
                "query": request.query,
                "terms": request.terms,
                "available": False,
                "reason": capability.status,
                "error": capability.detail,
                "matches": [],
            }

        # Unlike fetch(), a failure here has nothing to degrade to, so the
        # error propagates to the caller instead of becoming an empty result.
        sql, params = _build_search_sql(capability, request, table_schema)
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        candidates = _rank_candidates(columns, rows, request.terms)

        result = {
            "query": request.query,
            "terms": request.terms,
            "available": True,
            "signal_type": request.signal_type,
            "searched_columns": capability.selectable(SEARCH_COLUMNS),
            "matched_table_count": len(candidates),
            "matches": candidates[: request.limit],
            "truncated": len(rows) >= SEARCH_SCAN_LIMIT
            or len(candidates) > request.limit,
        }
        unsearched = capability.missing(SEARCH_COLUMNS)
        if unsearched:
            result["unsearched_columns"] = unsearched
        return result


def _probe(cursor) -> Capability:
    """Read the view's column set, classifying why it is unusable."""
    try:
        cursor.execute(f"DESC TABLE {VIEW}")
        columns = frozenset(str(row[0]) for row in cursor.fetchall())
    except Error as e:
        errno = getattr(e, "errno", None)
        if errno == ERRNO_TABLE_NOT_FOUND:
            return Capability("unavailable", detail=str(e))
        if errno in ERRNO_PERMISSION_DENIED:
            return Capability("permission_denied", detail=str(e))
        return Capability("error", detail=str(e))
    return Capability("available", columns=columns)


def _unavailable(reason: str, detail: str | None) -> dict:
    return {
        "included": True,
        "available": False,
        "found": False,
        "reason": reason,
        "error": detail,
    }


def _row_profile(values: dict) -> dict:
    options, raw_options, options_error = _parse_json_column(
        values.get("semantic_options"), "semantic_options", dict
    )
    declarations, raw_declarations, declarations_error = _parse_json_column(
        values.get("entity_declarations"), "entity_declarations", list
    )

    profile = {
        "table_catalog": values.get("table_catalog"),
        "table_schema": values.get("table_schema"),
        "table_name": values.get("table_name"),
        "table_id": values.get("table_id"),
        "signal_type": values.get("signal_type"),
        "source": values.get("source"),
        "source_version": values.get("source_version"),
        "pipeline": values.get("pipeline"),
        "metadata_quality": values.get("metadata_quality"),
        "options": options or {},
    }
    if raw_options is not None:
        profile["raw_options"] = raw_options
        profile["options_parse_error"] = options_error
    if declarations is not None:
        profile["entity_declarations"] = declarations
    if raw_declarations is not None:
        profile["raw_entity_declarations"] = raw_declarations
        profile["entity_declarations_parse_error"] = declarations_error
    return profile
