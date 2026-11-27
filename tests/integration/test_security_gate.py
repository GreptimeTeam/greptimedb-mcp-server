"""The read-only gate holds only while its lexer delimits strings and comments
the way the server does.

Each query hides an INSERT behind a string or comment boundary that the gate
reads as non-code. If the server reads the boundary differently, it runs the
INSERT that the gate never saw.
"""

import pytest

from greptimedb_mcp_server.utils import security_gate

from .conftest import _connect

pytestmark = pytest.mark.integration

PROBE_TABLE = "it_gate_probe"
INSERT = f"INSERT INTO {PROBE_TABLE} VALUES (now(), 1)"


@pytest.fixture
def probe_table(db):
    cursor = db.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {PROBE_TABLE}")
    cursor.execute(f"CREATE TABLE {PROBE_TABLE} (ts TIMESTAMP TIME INDEX, v INT)")
    yield
    cursor.execute(f"DROP TABLE IF EXISTS {PROBE_TABLE}")


@pytest.mark.parametrize(
    "query",
    [
        f"SELECT 'a\\'; {INSERT}; -- '",
        f'SELECT "a\\"; {INSERT}; -- "',
        f"SELECT 1 -- x\r; {INSERT}",
        f"SELECT 1 # x\r; {INSERT}",
    ],
)
def test_query_passed_by_gate_does_not_write(db, probe_table, query):
    assert security_gate(query) == (False, "")

    conn = _connect()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.fetchall()
    finally:
        conn.close()

    cursor = db.cursor()
    cursor.execute(f"SELECT count(*) FROM {PROBE_TABLE}")
    assert cursor.fetchone()[0] == 0
