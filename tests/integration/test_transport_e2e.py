"""Black-box tests for the HTTP transport path."""

import json

import pytest

from .conftest import METRICS_TABLE, call_text, streamable_http_session

pytestmark = pytest.mark.integration


async def test_streamable_http_transport(seed):
    async with streamable_http_session() as client:
        payload = json.loads(
            await call_text(
                client,
                "execute_sql",
                {
                    "query": f"SELECT count(*) AS n FROM {METRICS_TABLE}",
                    "format": "json",
                },
            )
        )
    assert payload["data"][0]["n"] == seed.row_count


async def test_streamable_http_with_dns_rebinding_protection(seed):
    async with streamable_http_session(
        **{"--allowed-hosts": "127.0.0.1:*,localhost:*"}
    ) as client:
        result = json.loads(await call_text(client, "health_check"))
    assert result["status"] == "healthy"
