"""Fixtures for black-box tests against a live GreptimeDB instance.

Seed data is loaded over a direct MySQL connection rather than through the
server, so a broken server cannot make its own fixtures pass.

Deselected by default; run with `pytest -m integration`.
"""

import os
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import mysql.connector
import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

HOST = os.getenv("GREPTIMEDB_IT_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("GREPTIMEDB_IT_PORT", "4002"))
HTTP_PORT = int(os.getenv("GREPTIMEDB_IT_HTTP_PORT", "4000"))
DATABASE = "public"

METRICS_TABLE = "it_cpu_metrics"
CREDENTIALS_TABLE = "it_credentials"

DECLARED_EDGES_TABLE = "greptime_private.semantic_relationships_declared"
# Declared edges are inserted directly, so the graph has both a RED-bearing
# relationship type and one without needing OTLP traces to pair up.
GRAPH_EDGES = (
    ("service", "it-frontend", "service", "it-checkout", "calls", 100, 7),
    ("service", "it-checkout", "service", "it-payment", "calls", 100, 45),
    ("service", "it-checkout", "k8s.pod", "it-pod-a", "runs_on", None, None),
    ("k8s.pod", "it-pod-a", "k8s.node", "it-node-1", "runs_on", None, None),
)

METRIC_HOSTS = ("host-a", "host-b")
METRIC_POINTS = 12
METRIC_INTERVAL_MS = 10_000

SECRET_PASSWORD = "hunter2"
SECRET_API_KEY = "sk-live-secret"
MASK_PLACEHOLDER = "******"


@dataclass(frozen=True)
class SeedData:
    base_ms: int
    hosts: tuple[str, ...]
    points_per_host: int
    # False before GreptimeDB 1.3, which is where entity options were added.
    entity_declared: bool = False
    graph_seeded: bool = False

    @property
    def row_count(self) -> int:
        return len(self.hosts) * self.points_per_host

    @property
    def earliest_ms(self) -> int:
        return self.base_ms - (self.points_per_host - 1) * METRIC_INTERVAL_MS


def _connect():
    return mysql.connector.connect(
        host=HOST,
        port=MYSQL_PORT,
        user="",
        password="",
        database=DATABASE,
    )


@pytest.fixture(scope="session")
def db():
    conn = _connect()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def seed(db):
    cursor = db.cursor()
    for table in (METRICS_TABLE, CREDENTIALS_TABLE):
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    # The semantic options make the table visible to search_table_semantics.
    # Entity declarations are added separately: the option key does not exist
    # before GreptimeDB 1.3 and CREATE TABLE rejects it outright.
    cursor.execute(f"""CREATE TABLE {METRICS_TABLE} (
            ts TIMESTAMP TIME INDEX,
            host STRING PRIMARY KEY,
            cpu DOUBLE
        ) WITH (
            'greptime.semantic.signal_type' = 'metric',
            'greptime.semantic.metric.type' = 'gauge',
            'greptime.semantic.metric.unit' = 'percent'
        )""")
    try:
        cursor.execute(
            f"ALTER TABLE {METRICS_TABLE} "
            "SET 'greptime.semantic.entity.host.id' = 'host'"
        )
        entity_declared = True
    except mysql.connector.Error:
        entity_declared = False
    # `password` is a reserved word in GreptimeDB's parser, hence user_password.
    # Both column names still match the default masking patterns.
    cursor.execute(f"""CREATE TABLE {CREDENTIALS_TABLE} (
            ts TIMESTAMP TIME INDEX,
            username STRING PRIMARY KEY,
            user_password STRING,
            api_key STRING
        )""")

    graph_seeded = _seed_declared_edges(cursor)

    base_ms = int(time.time() * 1000)
    metric_rows = [
        (base_ms - i * METRIC_INTERVAL_MS, host, 50.0 + i)
        for host in METRIC_HOSTS
        for i in range(METRIC_POINTS)
    ]
    cursor.executemany(
        f"INSERT INTO {METRICS_TABLE} (ts, host, cpu) VALUES (%s, %s, %s)",
        metric_rows,
    )
    cursor.execute(
        f"INSERT INTO {CREDENTIALS_TABLE} (ts, username, user_password, api_key) "
        f"VALUES (%s, %s, %s, %s)",
        (base_ms, "alice", SECRET_PASSWORD, SECRET_API_KEY),
    )
    db.commit()

    yield SeedData(
        base_ms=base_ms,
        hosts=METRIC_HOSTS,
        points_per_host=METRIC_POINTS,
        entity_declared=entity_declared,
        graph_seeded=graph_seeded,
    )

    for table in (METRICS_TABLE, CREDENTIALS_TABLE):
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    if graph_seeded:
        # Delete only the seeded edges: the table is shared and this suite did
        # not create it.
        for edge in GRAPH_EDGES:
            cursor.execute(
                f"DELETE FROM {DECLARED_EDGES_TABLE} WHERE src_id = %s AND dst_id = %s",
                (edge[1], edge[3]),
            )
    db.commit()


def _seed_declared_edges(cursor) -> bool:
    """Insert declared edges, reporting False when the graph does not exist."""
    columns = (
        "observed_at, src_type, src_id, dst_type, dst_id, rel_type, provenance, "
        "scope, generation_id, confidence, request_count, error_count"
    )
    try:
        for (
            src_type,
            src_id,
            dst_type,
            dst_id,
            rel_type,
            requests,
            errors,
        ) in GRAPH_EDGES:
            cursor.execute(
                f"INSERT INTO {DECLARED_EDGES_TABLE} ({columns}) VALUES "
                "(now(), %s, %s, %s, %s, %s, 'declared', '', '', 1.0, %s, %s)",
                (src_type, src_id, dst_type, dst_id, rel_type, requests, errors),
            )
    except mysql.connector.Error:
        return False
    return True


def server_argv(**overrides) -> list[str]:
    """Build the CLI argv for a server subprocess pointed at the test instance.

    Audit logging is left enabled on purpose, so every tool call here also
    exercises the audit path wrapped around each tool.
    """
    flags = {
        "--host": HOST,
        "--port": str(MYSQL_PORT),
        "--http-port": str(HTTP_PORT),
        "--database": DATABASE,
    }
    flags.update(overrides)
    argv = []
    for flag, value in flags.items():
        argv += [flag, value]
    return argv


@asynccontextmanager
async def stdio_session(**overrides):
    """Run the server over stdio and yield an initialized MCP client session."""
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "greptimedb_mcp_server.server", *server_argv(**overrides)],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise TimeoutError(f"MCP server did not start listening on port {port}")


@asynccontextmanager
async def streamable_http_session(**overrides):
    """Run the server over streamable-http and yield an initialized session."""
    port = _free_port()
    argv = server_argv(
        **{
            "--transport": "streamable-http",
            "--listen-host": "127.0.0.1",
            "--listen-port": str(port),
            **overrides,
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "greptimedb_mcp_server.server", *argv]
    )
    try:
        _wait_for_port(port)
        async with streamable_http_client(f"http://127.0.0.1:{port}/mcp") as (
            read,
            write,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session
    finally:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


async def call_text(session: ClientSession, name: str, arguments=None) -> str:
    """Call a tool and return its text content."""
    result = await session.call_tool(name, arguments or {})
    assert result.content, f"tool {name} returned no content"
    return "\n".join(block.text for block in result.content if block.type == "text")
