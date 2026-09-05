"""Reads the semantic graph in `greptime_private`.

Two computed views, derived at read time: `semantic_entities` holds the nodes
and `semantic_relationships` the witnessed edges. Both arrived in GreptimeDB
1.3, so `GraphView` decides whether the graph is usable at all before the
server offers a tool for it.

Rows are observations in 60-second buckets, so one logical edge appears once
per bucket it was seen in. Reads here aggregate across the requested window,
which is why the window is a required argument rather than a default.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from mysql.connector import Error

ENTITIES_VIEW = "greptime_private.semantic_entities"
RELATIONSHIPS_VIEW = "greptime_private.semantic_relationships"

# Read verbatim, so a view missing any of them cannot be queried the way this
# module queries it. These are the GreptimeDB 1.3 columns; the graph does not
# exist before that, so there is no older shape to degrade to.
ENTITY_COLUMNS = (
    "entity_type",
    "entity_id",
    "entity_id_attrs",
    "scope",
    "descriptive",
    "source_tables",
)
ENTITY_REQUIRED_COLUMNS = frozenset({"observed_at", "fresh_until", *ENTITY_COLUMNS})

RELATIONSHIP_IDENTITY_COLUMNS = (
    "src_type",
    "src_id",
    "dst_type",
    "dst_id",
    "rel_type",
    "provenance",
)
# Summed over the buckets in the window.
RED_COLUMNS = (
    "request_count",
    "unmatched_count",
    "error_count",
    "duration_sum",
    "duration_count",
)
RELATIONSHIP_REQUIRED_COLUMNS = frozenset(
    {
        "observed_at",
        "fresh_until",
        "confidence",
        "duration_max",
        *RELATIONSHIP_IDENTITY_COLUMNS,
        *RED_COLUMNS,
    }
)

# Columns holding JSON. Everything else is returned verbatim: decoding an
# identifier would turn "123" into a number and "null" into nothing, and the
# caller needs the exact string back to query with it.
JSON_COLUMNS = frozenset(
    {"entity_id_attrs", "descriptive", "source_tables", "attributes"}
)

VIEWS = ("summary", "entities", "relationships")

ENTITY_FILTERS = ("entity_type", "entity_id", "scope")
RELATIONSHIP_FILTERS = (
    "rel_type",
    "src_type",
    "src_id",
    "dst_type",
    "dst_id",
    "provenance",
)
# Filters that name a specific node. A zero-result query that used one is most
# often a wrong identifier rather than an absent relationship.
ID_FILTERS = ("entity_id", "src_id", "dst_id")

MAX_LIMIT = 500
DEFAULT_LIMIT = 100

OBSERVATION_BUCKET_SECONDS = 60

ERRNO_TABLE_NOT_FOUND = 1146
ERRNO_PERMISSION_DENIED = frozenset({1044, 1045, 1142, 1143, 1227})


@dataclass(frozen=True)
class GraphCapability:
    """Whether the graph can be read, and why not when it cannot."""

    status: str
    entity_columns: frozenset[str] = frozenset()
    relationship_columns: frozenset[str] = frozenset()
    detail: str | None = None

    @property
    def available(self) -> bool:
        return self.status == "available"

    @property
    def conclusive(self) -> bool:
        """Whether the probe reached an answer about the server itself.

        An inconclusive probe -- the database was unreachable, or failed in a
        way this module does not recognise -- says nothing about the graph, so
        it is neither cached nor allowed to withdraw the tool.
        """
        return self.status in (
            "available",
            "unavailable",
            "permission_denied",
            "incompatible_schema",
        )


@dataclass(frozen=True)
class TimeWindow:
    """A half-open [start, end) range over `observed_at`."""

    start: datetime
    end: datetime

    @classmethod
    def parse(cls, start_time: str, end_time: str) -> "TimeWindow":
        start = _parse_timestamp(start_time, "start_time")
        end = _parse_timestamp(end_time, "end_time")
        if start >= end:
            raise ValueError("start_time must be earlier than end_time")
        return cls(start=start, end=end)

    @property
    def params(self) -> list[str]:
        """Bind values that carry their offset.

        A literal without one is read in the session time zone, so the same
        window would select different rows depending on GREPTIMEDB_TIMEZONE.
        """
        return [self.start.isoformat(), self.end.isoformat()]

    def describe(self) -> dict:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "bounds": "[start, end)",
            "time_field": "observed_at",
            "observation_bucket_seconds": OBSERVATION_BUCKET_SECONDS,
        }


def _parse_timestamp(value: str, name: str) -> datetime:
    """Read an RFC3339 timestamp, treating a naive one as UTC."""
    if not value or not str(value).strip():
        raise ValueError(f"{name} is required")
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f"Invalid {name}: {value}. Use an RFC3339 timestamp such as "
            "2026-09-05T07:00:00Z"
        ) from None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class GraphRequest:
    """A validated graph query."""

    view: str
    window: TimeWindow
    filters: dict
    limit: int

    @classmethod
    def parse(
        cls,
        view: str,
        start_time: str,
        end_time: str,
        limit: int = DEFAULT_LIMIT,
        **filters,
    ) -> "GraphRequest":
        if view not in VIEWS:
            raise ValueError(
                f"Invalid view: {view}. Must be one of: {', '.join(VIEWS)}"
            )
        allowed = ENTITY_FILTERS if view == "entities" else RELATIONSHIP_FILTERS
        applied = {}
        for name, value in filters.items():
            if value in (None, ""):
                continue
            if view == "summary":
                raise ValueError(f"view=summary takes no filters, got {name}")
            if name not in allowed:
                raise ValueError(
                    f"Filter {name} does not apply to view={view}. "
                    f"Available: {', '.join(allowed)}"
                )
            applied[name] = value
        return cls(
            view=view,
            window=TimeWindow.parse(start_time, end_time),
            filters=applied,
            limit=max(1, min(limit, MAX_LIMIT)),
        )

    @property
    def names_an_id(self) -> bool:
        return any(name in ID_FILTERS for name in self.filters)


def _decode_json(name: str, value):
    if name not in JSON_COLUMNS or not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _row_dict(columns: list[str], row) -> dict:
    return {name: _decode_json(name, value) for name, value in zip(columns, row)}


def _filter_sql(filters: dict) -> tuple[list[str], list]:
    predicates = [f"{name} = %s" for name in filters]
    return predicates, list(filters.values())


def _no_match_guidance(request: GraphRequest) -> dict:
    """Say what to try next, without proposing an unfiltered dump.

    An unfiltered read of a large graph is the failure this tool exists to
    avoid, so the suggestion drops the identifier and keeps the type filters.
    """
    # The window is a required argument, so a next_query without it would not
    # run.
    window = {
        "start_time": request.window.start.isoformat(),
        "end_time": request.window.end.isoformat(),
    }
    if request.names_an_id:
        kept = {k: v for k, v in request.filters.items() if k not in ID_FILTERS}
        return {
            "reason": (
                "The supplied identifier may not be a canonical graph entity ID. "
                "IDs from alerts and telemetry are not interchangeable with them."
            ),
            "next_query": {"view": request.view, **kept, **window},
        }
    return {
        "reason": (
            "No rows matched in this window. The relationship may not have been "
            "witnessed, or the window may not cover it."
        ),
        "next_query": {"view": "summary", **window},
    }


@dataclass
class GraphView:
    """A handle on the two graph views that remembers whether they work."""

    capability: GraphCapability | None = field(default=None)

    def negotiate(self, cursor) -> GraphCapability:
        """Decide once per process whether the graph is usable."""
        if self.capability is not None:
            return self.capability
        capability = _probe(cursor)
        if capability.conclusive:
            self.capability = capability
        return capability

    def summary(self, cursor, window: TimeWindow) -> dict:
        """Report what the graph contains, without returning the graph.

        This is what a caller should read first: the shape of the graph is an
        interface fact, and learning it by paging through edges both costs a
        round trip and invites reading the result as a service call graph.
        """
        entity_types = self._entity_types(cursor, window)
        relationship_types = self._relationship_types(cursor, window)
        return {
            "view": "summary",
            "window": window.describe(),
            "entity_types": entity_types,
            "relationship_types": relationship_types,
            "entity_count": sum(item["count"] for item in entity_types),
            "relationship_count": sum(item["count"] for item in relationship_types),
            "complete": True,
        }

    def _entity_types(self, cursor, window: TimeWindow) -> list[dict]:
        cursor.execute(
            "SELECT entity_type, COUNT(*) AS entity_count FROM ("
            "  SELECT DISTINCT entity_type, entity_id"
            f"  FROM {ENTITIES_VIEW}"
            "   WHERE observed_at >= %s AND observed_at < %s"
            ") t GROUP BY entity_type ORDER BY entity_type",
            window.params,
        )
        return [{"type": row[0], "count": int(row[1])} for row in cursor.fetchall()]

    def _relationship_types(self, cursor, window: TimeWindow) -> list[dict]:
        cursor.execute(
            "SELECT rel_type, src_type, dst_type, COUNT(*) AS edge_count FROM ("
            "  SELECT DISTINCT rel_type, src_type, dst_type, src_id, dst_id, provenance"
            f"  FROM {RELATIONSHIPS_VIEW}"
            "   WHERE observed_at >= %s AND observed_at < %s"
            ") t GROUP BY rel_type, src_type, dst_type "
            "ORDER BY rel_type, src_type, dst_type",
            window.params,
        )
        # Endpoint types are reported as pairs. Reducing them to a set of
        # sources and a set of destinations would imply combinations that do
        # not exist: service->pod and pod->node would read as service->node.
        grouped: dict[str, dict] = {}
        for rel_type, src_type, dst_type, count in cursor.fetchall():
            entry = grouped.setdefault(
                rel_type, {"type": rel_type, "endpoints": [], "count": 0}
            )
            entry["endpoints"].append(
                {"source": src_type, "destination": dst_type, "count": int(count)}
            )
            entry["count"] += int(count)
        return list(grouped.values())

    def entities(self, cursor, request: GraphRequest) -> dict:
        """List distinct entities observed in the window."""
        predicates, params = _filter_sql(request.filters)
        where = " AND ".join(["observed_at >= %s", "observed_at < %s", *predicates])
        # descriptive is grouped rather than aggregated: MAX over a JSON
        # column would silently return one bucket's value for an entity whose
        # attributes changed inside the window.
        identity = ", ".join(ENTITY_COLUMNS)
        cursor.execute(
            f"SELECT {identity},"
            "        MIN(observed_at) AS first_seen,"
            "        MAX(observed_at) AS last_seen,"
            "        MAX(fresh_until) AS fresh_until"
            f" FROM {ENTITIES_VIEW} WHERE {where}"
            f" GROUP BY {identity}"
            " ORDER BY entity_type, entity_id"
            f" LIMIT {request.limit + 1}",
            [*request.window.params, *params],
        )
        return self._envelope(cursor, request)

    def relationships(self, cursor, request: GraphRequest) -> dict:
        """List edges observed in the window, aggregated across buckets.

        `attributes` is left out: it varies per observation, so grouping by it
        would split one edge into several rows and break the RED totals, while
        aggregating it would present one bucket's value as the edge's. Read it
        per observation with execute_sql.
        """
        aggregates = [f"SUM({column}) AS {column}" for column in RED_COLUMNS]
        aggregates.append("MAX(duration_max) AS duration_max")

        predicates, params = _filter_sql(request.filters)
        where = " AND ".join(["observed_at >= %s", "observed_at < %s", *predicates])
        identity = ", ".join(RELATIONSHIP_IDENTITY_COLUMNS)
        cursor.execute(
            f"SELECT {identity}, MAX(confidence) AS confidence, "
            + ", ".join(aggregates)
            + ", MIN(observed_at) AS first_seen, MAX(observed_at) AS last_seen"
            + ", MAX(fresh_until) AS fresh_until"
            f" FROM {RELATIONSHIPS_VIEW} WHERE {where}"
            f" GROUP BY {identity}"
            f" ORDER BY {_relationship_order(request)}"
            f" LIMIT {request.limit + 1}",
            [*request.window.params, *params],
        )
        return self._envelope(cursor, request)

    def _envelope(self, cursor, request: GraphRequest) -> dict:
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        complete = len(rows) <= request.limit
        items = [_row_dict(columns, row) for row in rows[: request.limit]]
        result = {
            "view": request.view,
            "status": "ok" if items else "no_match",
            "window": request.window.describe(),
            "applied_filters": dict(request.filters),
            "items": items,
            "item_count": len(items),
            "complete": complete,
        }
        if not items:
            result["guidance"] = _no_match_guidance(request)
        return result


def _relationship_order(request: GraphRequest) -> str:
    """Order edges by identity unless the caller asked for one RED-bearing type.

    Only `calls` edges carry request and error counts. Ordering a mixed result
    by them would sort every other relationship type to the bottom on a NULL
    and teach the caller that the graph is a service call graph.
    """
    if request.filters.get("rel_type") == "calls":
        return "error_count DESC, request_count DESC, src_id, dst_id"
    return "rel_type, src_type, src_id, dst_type, dst_id"


def _probe(cursor) -> GraphCapability:
    """Check both views exist, carry the columns used here, and can be read."""
    columns = {}
    for view in (ENTITIES_VIEW, RELATIONSHIPS_VIEW):
        try:
            cursor.execute(f"DESC TABLE {view}")
            columns[view] = frozenset(str(row[0]) for row in cursor.fetchall())
        except Error as e:
            return _classify(e)

    missing = ENTITY_REQUIRED_COLUMNS - columns[ENTITIES_VIEW]
    missing |= RELATIONSHIP_REQUIRED_COLUMNS - columns[RELATIONSHIPS_VIEW]
    if missing:
        return GraphCapability(
            "incompatible_schema",
            detail=f"missing columns: {', '.join(sorted(missing))}",
        )

    # DESC answers from the catalog, so it says nothing about whether this
    # account may read the derivation. A bounded read does.
    for view in (ENTITIES_VIEW, RELATIONSHIPS_VIEW):
        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM {view} "
                "WHERE observed_at >= now() - INTERVAL '1' MINUTE"
            )
            cursor.fetchall()
        except Error as e:
            return _classify(e)

    return GraphCapability(
        "available",
        entity_columns=columns[ENTITIES_VIEW],
        relationship_columns=columns[RELATIONSHIPS_VIEW],
    )


def _classify(error: Error) -> GraphCapability:
    errno = getattr(error, "errno", None)
    if errno == ERRNO_TABLE_NOT_FOUND:
        return GraphCapability("unavailable", detail=str(error))
    if errno in ERRNO_PERMISSION_DENIED:
        return GraphCapability("permission_denied", detail=str(error))
    return GraphCapability("error", detail=str(error))
