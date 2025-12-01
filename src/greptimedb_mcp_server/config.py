import argparse
from dataclasses import dataclass
import os


@dataclass
class Config:
    """
    Configuration for the greptimedb mcp server.
    """

    host: str
    """
    GreptimeDB host
    """

    port: int
    """
    GreptimeDB MySQL protocol port
    """

    user: str
    """
    GreptimeDB username
    """

    password: str
    """
    GreptimeDB password
    """

    database: str
    """
    GreptimeDB database name
    """

    time_zone: str
    """
    GreptimeDB session time zone
    """

    pool_size: int
    """
    Connection pool size
    """

    http_port: int
    """
    GreptimeDB HTTP API port
    """

    http_protocol: str
    """
    HTTP protocol (http or https)
    """

    mask_enabled: bool
    """
    Enable data masking for sensitive columns
    """

    mask_patterns: str
    """
    Additional sensitive column patterns (comma-separated)
    """

    @staticmethod
    def from_env_arguments() -> "Config":
        """
        Parse command line arguments.
        """
        parser = argparse.ArgumentParser(description="GreptimeDB MCP Server")

        parser.add_argument(
            "--host",
            type=str,
            help="GreptimeDB host",
            default=os.getenv("GREPTIMEDB_HOST", "localhost"),
        )

        parser.add_argument(
            "--port",
            type=int,
            help="GreptimeDB MySQL protocol port",
            default=os.getenv("GREPTIMEDB_PORT", 4002),
        )

        parser.add_argument(
            "--database",
            type=str,
            help="GreptimeDB connect database name",
            default=os.getenv("GREPTIMEDB_DATABASE", "public"),
        )

        parser.add_argument(
            "--user",
            type=str,
            help="GreptimeDB username",
            default=os.getenv("GREPTIMEDB_USER", ""),
        )

        parser.add_argument(
            "--password",
            type=str,
            help="GreptimeDB password",
            default=os.getenv("GREPTIMEDB_PASSWORD", ""),
        )

        parser.add_argument(
            "--timezone",
            type=str,
            help="GreptimeDB session time zone",
            default=os.getenv("GREPTIMEDB_TIMEZONE", ""),
        )

        parser.add_argument(
            "--pool-size",
            type=int,
            help="Connection pool size (default: 5)",
            default=int(os.getenv("GREPTIMEDB_POOL_SIZE", "5")),
        )

        parser.add_argument(
            "--http-port",
            type=int,
            help="GreptimeDB HTTP API port (default: 4000)",
            default=int(os.getenv("GREPTIMEDB_HTTP_PORT", "4000")),
        )

        parser.add_argument(
            "--http-protocol",
            type=str,
            choices=["http", "https"],
            help="HTTP protocol for API calls (default: http)",
            default=os.getenv("GREPTIMEDB_HTTP_PROTOCOL", "http"),
        )

        parser.add_argument(
            "--mask-enabled",
            type=lambda x: x.lower() not in ("false", "0", "no"),
            help="Enable data masking for sensitive columns (default: true)",
            default=os.getenv("GREPTIMEDB_MASK_ENABLED", "true"),
        )

        parser.add_argument(
            "--mask-patterns",
            type=str,
            help="Additional sensitive column patterns (comma-separated)",
            default=os.getenv("GREPTIMEDB_MASK_PATTERNS", ""),
        )

        args = parser.parse_args()
        return Config(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            time_zone=args.timezone,
            pool_size=args.pool_size,
            http_port=args.http_port,
            http_protocol=args.http_protocol,
            mask_enabled=args.mask_enabled,
            mask_patterns=args.mask_patterns,
        )
