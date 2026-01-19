import asyncio
import sys

# Windows: use SelectorEventLoop for HTTP transports (signal handling),
# but keep ProactorEventLoop for stdio (pipe I/O support)
if sys.platform == "win32" and any(t in sys.argv for t in ("sse", "streamable-http")):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def main():
    """Main entry point for the package."""
    from . import server

    try:
        server.main()
    except KeyboardInterrupt:
        print("\nReceived Ctrl+C, shutting down...")
    except asyncio.CancelledError:
        print("\nServer shutdown complete.")


# Expose important items at package level
__all__ = ["main"]
