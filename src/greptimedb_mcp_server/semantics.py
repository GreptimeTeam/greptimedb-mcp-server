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


# Identifier tokens: an ALL-CAPS run, a word, or a number. Splitting on case
# transitions as well as separators is what lets `usedMemoryBytes` and
# `used_memory_bytes` tokenize alike, and keeps `nodeCPUSeconds` from becoming
# one opaque token.
_TOKEN = re.compile(r"[A-Z]+(?![a-z])|[A-Za-z][a-z0-9]*|[0-9]+")
_SLASH_PAIR = re.compile(r"\b([A-Za-z])\s*/\s*([A-Za-z])\b")

# Legacy metric names abbreviate an I/O direction as a single letter beside the
# subsystem, so `system_io_w_s` is a write metric. Each entry maps the word a
# caller searches for to the adjacent token pair that stands for it. Ported
# from GreptimeTeam/agent-rca-bench, where this is what made concept search
# work on legacy metric schemas.
TOKEN_SYNONYMS = {"write": ("io", "w"), "read": ("io", "r")}

MIN_PREFIX_TERM = 3


def _tokens(text: str) -> list[str]:
    """Split an identifier or a query into comparable tokens.

    A slash between two single letters is not a separator: `I/O` is one term,
    and splitting it leaves two one-character tokens that the term filter then
    discards, so a search for it would have nothing to look for.
    """
    joined = _SLASH_PAIR.sub(r"\1\2", text)
    return [token.lower() for token in _TOKEN.findall(joined)]


def _expanded_tokens(text: str) -> set[str]:
    """Tokens of `text`, plus the words its abbreviations stand for."""
    tokens = _tokens(text)
    expanded = set(tokens)
    adjacent = set(zip(tokens, tokens[1:]))
    for word, pair in TOKEN_SYNONYMS.items():
        if pair in adjacent:
            expanded.add(word)
    return expanded


def _search_terms(query: str) -> list[str]:
    """Split a concept query into distinct searchable terms."""
    terms = (
        term for term in _tokens(query) if len(term) > 1 and term not in STOP_WORDS
    )
    return list(dict.fromkeys(terms))[:MAX_SEARCH_TERMS]


def _matched_terms(terms: list[str], searchable: str) -> list[str]:
    """Return the terms a candidate matched, for ranking.

    One rule, applied per token: the term is the token, or -- from three
    characters up -- a prefix of it. Matching per token rather than across the
    whole string is what keeps `geo` out of `range of`, and a prefix rather
    than a substring is what keeps it out of `rangeof` too.
    """
    tokens = _expanded_tokens(searchable)
    return [
        term
        for term in terms
        if term in tokens
        or (
            len(term) >= MIN_PREFIX_TERM
            and any(token.startswith(term) for token in tokens)
        )
    ]


def _recall_patterns(term: str) -> list[str]:
    """LIKE patterns that must reach every row `_matched_terms` would accept.

    A token or prefix match implies the term appears as a substring, so one
    pattern covers it. A synonym does not: nothing in `system_io_w_s` contains
    `write`, so searching for the abbreviation is what puts the row in front of
    the matcher.
    """
    patterns = [term]
    pair = TOKEN_SYNONYMS.get(term)
    if pair:
        patterns.append(pair[0])
    return patterns


def search_failure(request: SearchRequest, reason: str, detail: str | None) -> dict:
    """The envelope every unsuccessful search returns.

    Success and failure share query, terms, signal_type, available,
    matched_table_count and matches, so one shape parses either.
    """
    return {
        "query": request.query,
        "terms": request.terms,
        "signal_type": request.signal_type,
        "available": False,
        "reason": reason,
        "error": detail,
        "matched_table_count": 0,
        "matches": [],
    }


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
        if profile.get("reason") == "error":
            return [
                f"Reading {VIEW} failed; the error field says why. This is a "
                "failed read, not a statement about what this server supports, "
                "and a retry may succeed."
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
    # Only a hand-written declaration can drop the qualifier: it replaces the
    # convention for that entity type outright, so one that omits id_qualifier
    # loses the qualifier the convention would have applied. Naming the
    # affected types keeps this off every other table's profile.
    unqualified = sorted(
        {
            str(item.get("entity_type"))
            for item in declarations
            if isinstance(item, dict)
            and item.get("origin") == "declared"
            and not item.get("id_qualifier")
            and item.get("entity_type")
        }
    )
    if unqualified:
        hints.append(
            f"These declarations are explicit and name no id_qualifier: "
            f"{', '.join(unqualified)}. An explicit declaration replaces the "
            "built-in convention for its entity type outright, so any "
            "qualifier the convention would have applied is dropped, and one "
            "process can then appear under two ids. Compare origin and "
            "id_qualifier before treating two ids as two things."
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
    """Build the candidate query and its bound parameters.

    This LIKE filter is a deliberate over-approximation whose only job is to
    bound what gets read; `_matched_terms` is the single definition of a match
    and decides what is returned. Every pattern here is a superset of what that
    rule accepts, so narrowing the read never hides a row the ranking wanted.
    """
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
        for pattern in _recall_patterns(term):
            # COALESCE keeps a NULL column from making the whole OR group NULL,
            # which would drop rows that matched on another column.
            clauses = [
                f"LOWER(COALESCE({column}, '')) LIKE %s" for column in searchable
            ]
            term_clauses.append(f"({' OR '.join(clauses)})")
            params.extend([f"%{pattern}%"] * len(clauses))
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


def _json_leaves(value) -> list[str]:
    """Collect the leaf values of a decoded JSON payload."""
    if isinstance(value, dict):
        return [leaf for item in value.values() for leaf in _json_leaves(item)]
    if isinstance(value, list):
        return [leaf for item in value for leaf in _json_leaves(item)]
    return [str(value)] if value not in (None, "") else []


def _searchable_text(values: dict) -> str:
    """Assemble the text a candidate is ranked on.

    Only the values of the JSON columns take part. Their key names are the
    schema's own vocabulary -- metric.type, origin, entity_type -- so ranking
    on them gives a query like "metric type unit" a perfect score against every
    metric table and collapses the ordering to table name.
    """
    parts = [str(values["table_name"])] if values.get("table_name") else []
    for column in ("semantic_options", "entity_declarations"):
        raw = values.get(column)
        if not raw:
            continue
        try:
            parts.extend(_json_leaves(json.loads(raw)))
        except (TypeError, json.JSONDecodeError):
            parts.append(str(raw))
    return " ".join(parts)


def _candidate(values: dict, terms: list[str]) -> dict | None:
    searchable = _searchable_text(values)
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
            return search_failure(request, capability.status, capability.detail)

        # Unlike fetch(), a failure here has nothing to degrade to, so the
        # error propagates to the caller instead of becoming an empty result.
        sql, params = _build_search_sql(capability, request, table_schema)
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        candidates = _rank_candidates(columns, rows, request.terms)

        # ORDER BY in the scan makes the truncation deterministic rather than
        # arbitrary; the ranking below decides the order that is returned.
        scan_truncated = len(rows) >= SEARCH_SCAN_LIMIT
        result = {
            "query": request.query,
            "terms": request.terms,
            "available": True,
            "signal_type": request.signal_type,
            "searched_columns": capability.selectable(SEARCH_COLUMNS),
            "matched_table_count": len(candidates),
            "matches": candidates[: request.limit],
            "truncated": scan_truncated or len(candidates) > request.limit,
        }
        if scan_truncated:
            result["ranking_note"] = (
                f"More than {SEARCH_SCAN_LIMIT} tables matched, so ranking saw "
                "only the first that many by table name. These are not "
                "necessarily the best matches; narrow the query or set "
                "signal_type."
            )
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
