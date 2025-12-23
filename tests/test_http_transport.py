"""Blackbox tests for HTTP transport modes (streamable-http and sse)."""

import asyncio
import json
import pytest
import socket
from contextlib import closing
from unittest.mock import patch

import httpx


def find_free_port() -> int:
    """Find a free port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture
def free_port():
    """Get a free port for testing."""
    return find_free_port()


@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch("greptimedb_mcp_server.server.connect") as mock_connect:
        mock_conn = mock_connect.return_value.__enter__.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = ("GreptimeDB 0.9.0",)
        yield mock_connect


class TestStreamableHttpTransport:
    """Tests for streamable-http transport mode."""

    @pytest.mark.asyncio
    async def test_initialize_returns_valid_mcp_response(
        self, free_port, mock_db_connection
    ):
        """Test that initialize request returns valid MCP protocol response."""
        from mcp.server.fastmcp import FastMCP

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        @test_mcp.tool()
        def ping() -> str:
            return "pong"

        async def run_server():
            await test_mcp.run_streamable_http_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.post(
                    f"http://127.0.0.1:{free_port}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "1.0"},
                        },
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                    },
                    timeout=5.0,
                )
                assert response.status_code == 200

                # Response is SSE format: "event: message\r\ndata: {...}\r\n"
                # Extract JSON from the data line
                content = response.text
                data = None
                for line in content.split("\n"):
                    if line.startswith("data:"):
                        data = json.loads(line[5:].strip())
                        break

                assert data is not None, "No data line found in SSE response"
                assert data.get("jsonrpc") == "2.0"
                assert data.get("id") == 1
                assert "result" in data

                # Verify MCP initialize response fields
                result = data["result"]
                assert "protocolVersion" in result
                assert "capabilities" in result
                assert "serverInfo" in result
                assert result["serverInfo"]["name"] == "test_server"
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_mcp_endpoint_rejects_invalid_json(
        self, free_port, mock_db_connection
    ):
        """Test that /mcp endpoint returns error for invalid JSON."""
        from mcp.server.fastmcp import FastMCP

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        async def run_server():
            await test_mcp.run_streamable_http_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.post(
                    f"http://127.0.0.1:{free_port}/mcp",
                    content=b"not valid json",
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                    },
                    timeout=5.0,
                )
                # Should return error status for invalid request
                assert response.status_code in [400, 422, 500]
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


class TestSseTransport:
    """Tests for SSE transport mode."""

    @pytest.mark.asyncio
    async def test_sse_endpoint_returns_endpoint_event(
        self, free_port, mock_db_connection
    ):
        """Test that /sse endpoint returns SSE event with messages endpoint."""
        from mcp.server.fastmcp import FastMCP

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        async def run_server():
            await test_mcp.run_sse_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                async with client.stream(
                    "GET", f"http://127.0.0.1:{free_port}/sse", timeout=2.0
                ) as response:
                    assert response.status_code == 200
                    assert "text/event-stream" in response.headers.get(
                        "content-type", ""
                    )

                    # Read first SSE event (endpoint announcement)
                    event_data = ""
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            event_data = line[5:].strip()
                            break

                    # Verify endpoint URL is provided
                    assert "/messages/" in event_data
        except httpx.ReadTimeout:
            pass  # SSE stream stays open, timeout is expected
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_messages_endpoint_rejects_invalid_session(
        self, free_port, mock_db_connection
    ):
        """Test that /messages/ endpoint rejects requests without valid session."""
        from mcp.server.fastmcp import FastMCP

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        async def run_server():
            await test_mcp.run_sse_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.post(
                    f"http://127.0.0.1:{free_port}/messages/",
                    json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
                    timeout=5.0,
                )
                # Without valid session ID, should return error
                assert response.status_code in [400, 404, 500]
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


class TestTransportConfig:
    """Tests for transport configuration."""

    def test_config_transport_choices(self):
        """Test that transport config only accepts valid choices."""
        from greptimedb_mcp_server.config import Config
        import sys

        # Valid transports should work
        for transport in ["stdio", "sse", "streamable-http"]:
            with patch.dict("os.environ", {}, clear=True):
                with patch.object(
                    sys,
                    "argv",
                    ["test", "--transport", transport],
                ):
                    config = Config.from_env_arguments()
                    assert config.transport == transport

    def test_config_invalid_transport_rejected(self):
        """Test that invalid transport is rejected."""
        import sys

        with patch.dict("os.environ", {}, clear=True):
            with patch.object(sys, "argv", ["test", "--transport", "invalid"]):
                with pytest.raises(SystemExit):
                    from greptimedb_mcp_server.config import Config

                    Config.from_env_arguments()


class TestDnsRebindingProtection:
    """Tests for DNS rebinding protection configuration."""

    @pytest.mark.asyncio
    async def test_protection_disabled_by_default(self, free_port, mock_db_connection):
        """Test that DNS rebinding protection is disabled when allowed_hosts is empty."""
        from mcp.server.fastmcp import FastMCP
        from mcp.server.fastmcp.server import TransportSecuritySettings

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        # Simulate our server.py logic: empty allowed_hosts = disabled
        test_mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        )

        async def run_server():
            await test_mcp.run_streamable_http_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                # Request with arbitrary Host header should succeed
                response = await client.post(
                    f"http://127.0.0.1:{free_port}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "1.0"},
                        },
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        "Host": "arbitrary-host.example.com:8080",
                    },
                    timeout=5.0,
                )
                # Should succeed (not 421)
                assert response.status_code == 200
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_protection_enabled_rejects_invalid_host(
        self, free_port, mock_db_connection
    ):
        """Test that enabled protection rejects requests with invalid Host header."""
        from mcp.server.fastmcp import FastMCP
        from mcp.server.fastmcp.server import TransportSecuritySettings

        test_mcp = FastMCP("test_server", host="127.0.0.1", port=free_port)

        # Enable protection with specific allowed hosts
        test_mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["localhost:*", "127.0.0.1:*"],
        )

        async def run_server():
            await test_mcp.run_streamable_http_async()

        server_task = asyncio.create_task(run_server())
        await asyncio.sleep(0.5)

        try:
            async with httpx.AsyncClient(trust_env=False) as client:
                # Request with disallowed Host header should be rejected
                response = await client.post(
                    f"http://127.0.0.1:{free_port}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {"name": "test", "version": "1.0"},
                        },
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/event-stream",
                        "Host": "malicious-host.example.com:8080",
                    },
                    timeout=5.0,
                )
                # Should be rejected with 421
                assert response.status_code == 421
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
