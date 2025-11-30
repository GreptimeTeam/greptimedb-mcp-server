import os
from unittest.mock import patch
from greptimedb_mcp_server.config import Config


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
