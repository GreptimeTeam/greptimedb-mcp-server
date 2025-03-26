import pytest
import sys


# Mock classes for MySQL connection
class MockCursor:
    def __init__(self):
        self.query = ""
        self.rowcount = 2

    def execute(self, query):
        self.query = query

    def fetchall(self):
        if "SHOW TABLES" in self.query.upper():
            return [("users",), ("orders",)]
        elif "SELECT" in self.query.upper():
            return [(1, "John"), (2, "Jane")]
        return []

    @property
    def description(self):
        if "SHOW TABLES" in self.query.upper():
            return [("table_name", None)]
        elif "SELECT" in self.query.upper():
            return [("id", None), ("name", None)]
        return []

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnection:
    def __init__(self, *args, **kwargs):
        pass

    def cursor(self):
        return MockCursor()

    def commit(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockMySQLModule:
    """Mock for entire mysql.connector module"""

    @staticmethod
    def connect(*args, **kwargs):
        return MockConnection(*args, **kwargs)

    class Error(Exception):
        """Mock MySQL error class"""

        pass


def pytest_configure(config):
    """
    Called at the start of the pytest session, before tests are collected.
    This is where we apply our global patches before any imports happen.
    """
    # Create and store original modules if they exist
    original_mysql = sys.modules.get("mysql.connector")

    # Create mock MySQL module
    sys.modules["mysql.connector"] = MockMySQLModule

    # Store the original function for later import and patching
    config._mysql_original = original_mysql


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Restore original modules after all tests are done"""
    if hasattr(session.config, "_mysql_original") and session.config._mysql_original:
        sys.modules["mysql.connector"] = session.config._mysql_original
