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

        args = parser.parse_args()
        return Config(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        )
