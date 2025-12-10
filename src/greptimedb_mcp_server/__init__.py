import asyncio
import sys

if "-m" not in sys.argv:
    from . import server


def main():
    """Main entry point for the package."""
    try:
        server.main()
    except KeyboardInterrupt:
        print("\nReceived Ctrl+C, shutting down...")
    except asyncio.CancelledError:
        print("\nServer shutdown complete.")


# Expose important items at package level
__all__ = ["main", "server"]
