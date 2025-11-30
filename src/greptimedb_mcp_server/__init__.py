import sys

if "-m" not in sys.argv:
    from . import server


def main():
    """Main entry point for the package."""
    server.main()


# Expose important items at package level
__all__ = ["main", "server"]
