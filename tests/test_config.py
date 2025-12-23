import os
from unittest.mock import patch
from greptimedb_mcp_server.config import Config, _parse_comma_separated


def test_config_default_values():
    """
    Test default configuration values
    """
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["script_name"]):
            config = Config.from_env_arguments()

            assert config.host == "localhost"
            assert config.port == 4002
            assert config.database == "public"
            assert config.user == ""
            assert config.password == ""
            assert config.time_zone == ""
            assert config.http_protocol == "http"
            assert config.mask_enabled is True
            assert config.mask_patterns == ""
            assert config.transport == "stdio"
            assert config.listen_host == "0.0.0.0"
            assert config.listen_port == 8080
            assert config.allowed_hosts == []
            assert config.allowed_origins == []


def test_config_env_variables():
    """
    Test configuration via environment variables
    """
    env_vars = {
        "GREPTIMEDB_HOST": "test-host",
        "GREPTIMEDB_PORT": "5432",
        "GREPTIMEDB_DATABASE": "test_db",
        "GREPTIMEDB_USER": "test_user",
        "GREPTIMEDB_PASSWORD": "test_password",
        "GREPTIMEDB_TIMEZONE": "test_tz",
        "GREPTIMEDB_HTTP_PROTOCOL": "https",
        "GREPTIMEDB_MASK_ENABLED": "false",
        "GREPTIMEDB_MASK_PATTERNS": "phone,address",
        "GREPTIMEDB_TRANSPORT": "streamable-http",
        "GREPTIMEDB_LISTEN_HOST": "127.0.0.1",
        "GREPTIMEDB_LISTEN_PORT": "3000",
        "GREPTIMEDB_ALLOWED_HOSTS": "localhost:*,127.0.0.1:*",
        "GREPTIMEDB_ALLOWED_ORIGINS": "http://localhost:*,https://example.com",
    }

    with patch.dict(os.environ, env_vars):
        with patch("sys.argv", ["script_name"]):
            config = Config.from_env_arguments()

            assert config.host == "test-host"
            assert config.port == 5432
            assert config.database == "test_db"
            assert config.user == "test_user"
            assert config.password == "test_password"
            assert config.time_zone == "test_tz"
            assert config.http_protocol == "https"
            assert config.mask_enabled is False
            assert config.mask_patterns == "phone,address"
            assert config.transport == "streamable-http"
            assert config.listen_host == "127.0.0.1"
            assert config.listen_port == 3000
            assert config.allowed_hosts == ["localhost:*", "127.0.0.1:*"]
            assert config.allowed_origins == [
                "http://localhost:*",
                "https://example.com",
            ]


def test_config_cli_arguments():
    """
    Test configuration via command-line arguments
    """
    cli_args = [
        "script_name",
        "--host",
        "cli-host",
        "--port",
        "9999",
        "--database",
        "cli_db",
        "--user",
        "cli_user",
        "--password",
        "cli_password",
        "--timezone",
        "cli_tz",
        "--http-protocol",
        "https",
        "--mask-enabled",
        "false",
        "--mask-patterns",
        "custom1,custom2",
        "--transport",
        "sse",
        "--listen-host",
        "192.168.1.1",
        "--listen-port",
        "9090",
        "--allowed-hosts",
        "my-service.namespace:*",
        "--allowed-origins",
        "http://my-app.example.com",
    ]

    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", cli_args):
            config = Config.from_env_arguments()

            assert config.host == "cli-host"
            assert config.port == 9999
            assert config.database == "cli_db"
            assert config.user == "cli_user"
            assert config.password == "cli_password"
            assert config.time_zone == "cli_tz"
            assert config.http_protocol == "https"
            assert config.mask_enabled is False
            assert config.mask_patterns == "custom1,custom2"
            assert config.transport == "sse"
            assert config.listen_host == "192.168.1.1"
            assert config.listen_port == 9090
            assert config.allowed_hosts == ["my-service.namespace:*"]
            assert config.allowed_origins == ["http://my-app.example.com"]


def test_config_precedence():
    """
    Test configuration precedence (CLI arguments override environment variables)
    """
    env_vars = {
        "GREPTIMEDB_HOST": "env-host",
        "GREPTIMEDB_PORT": "6666",
        "GREPTIMEDB_DATABASE": "env_db",
        "GREPTIMEDB_USER": "env_user",
        "GREPTIMEDB_PASSWORD": "env_password",
        "GREPTIMEDB_TIMEZONE": "env_tz",
        "GREPTIMEDB_HTTP_PROTOCOL": "http",
        "GREPTIMEDB_MASK_ENABLED": "true",
        "GREPTIMEDB_MASK_PATTERNS": "env_pattern",
        "GREPTIMEDB_TRANSPORT": "stdio",
        "GREPTIMEDB_LISTEN_HOST": "env-listen-host",
        "GREPTIMEDB_LISTEN_PORT": "1111",
    }

    cli_args = [
        "script_name",
        "--host",
        "cli-host",
        "--port",
        "9999",
        "--database",
        "cli_db",
        "--user",
        "cli_user",
        "--password",
        "cli_password",
        "--timezone",
        "cli_tz",
        "--http-protocol",
        "https",
        "--mask-enabled",
        "false",
        "--mask-patterns",
        "cli_pattern",
        "--transport",
        "streamable-http",
        "--listen-host",
        "cli-listen-host",
        "--listen-port",
        "2222",
    ]

    with patch.dict(os.environ, env_vars):
        with patch("sys.argv", cli_args):
            config = Config.from_env_arguments()

            assert config.host == "cli-host"
            assert config.port == 9999
            assert config.database == "cli_db"
            assert config.user == "cli_user"
            assert config.password == "cli_password"
            assert config.time_zone == "cli_tz"
            assert config.http_protocol == "https"
            assert config.mask_enabled is False
            assert config.mask_patterns == "cli_pattern"
            assert config.transport == "streamable-http"
            assert config.listen_host == "cli-listen-host"
            assert config.listen_port == 2222


class TestParseCommaSeparated:
    """Tests for _parse_comma_separated helper function."""

    def test_empty_and_whitespace(self):
        assert _parse_comma_separated("") == []
        assert _parse_comma_separated("   ") == []

    def test_single_value(self):
        assert _parse_comma_separated("localhost:*") == ["localhost:*"]
        assert _parse_comma_separated("  localhost:*  ") == ["localhost:*"]

    def test_multiple_values(self):
        assert _parse_comma_separated("  localhost:* , 127.0.0.1:*  ") == [
            "localhost:*",
            "127.0.0.1:*",
        ]

    def test_empty_items_filtered(self):
        assert _parse_comma_separated("localhost:*,,  ,127.0.0.1:*") == [
            "localhost:*",
            "127.0.0.1:*",
        ]
