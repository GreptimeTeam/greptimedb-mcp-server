import pytest
import sys


# Mock classes for MySQL connection
class MockCursor:
    def __init__(self):
        self.query = ""
        self.rowcount = 2
        self._results = []
        self._fetch_index = 0

    def execute(self, query, args=None):
        self.query = query
        self._fetch_index = 0
        # Pre-populate results based on query
        if "SHOW TABLES" in self.query.upper():
            self._results = [("users",), ("orders",)]
        elif "SHOW DATABASES" in self.query.upper():
            self._results = [("public",), ("greptime_private",)]
        elif "DESCRIBE" in self.query.upper():
            self._results = [
                ("id", "Int64", "", "PRI", "", ""),
                ("name", "String", "YES", "", "", ""),
                ("ts", "TimestampMillisecond", "", "TIME INDEX", "", ""),
            ]
        elif "VERSION()" in self.query.upper():
            self._results = [("GreptimeDB 0.9.0",)]
        elif "TQL" in self.query.upper():
            self._results = [
                ("2024-01-01 00:00:00", "host1", 0.5),
                ("2024-01-01 00:01:00", "host1", 0.6),
            ]
        elif "EXPLAIN" in self.query.upper():
            self._results = [
                ("GlobalLimitExec: skip=0, fetch=1000",),
                ("  SortExec: TopK(fetch=1000)",),
                ("    TableScan: users",),
            ]
        elif "ALIGN" in self.query.upper():
            # Range query
            self._results = [
                ("2024-01-01 00:00:00", "host1", 45.5),
                ("2024-01-01 00:05:00", "host1", 52.3),
            ]
        elif "GREPTIME_PRIVATE.PIPELINES" in self.query.upper():
            # Pipeline list query - returns empty by default
            self._results = []
        elif "SELECT" in self.query.upper():
            self._results = [(1, "John"), (2, "Jane")]
        else:
            self._results = []

    def fetchall(self):
        results = self._results[self._fetch_index :]
        self._fetch_index = len(self._results)
        return results

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        results = self._results[self._fetch_index : self._fetch_index + size]
        self._fetch_index += len(results)
        return results

    def fetchone(self):
        if self._fetch_index < len(self._results):
            result = self._results[self._fetch_index]
            self._fetch_index += 1
            return result
        return None

    @property
    def description(self):
        if "SHOW TABLES" in self.query.upper():
            return [("table_name", None)]
        elif "SHOW DATABASES" in self.query.upper():
            return [("Databases", None)]
        elif "DESCRIBE" in self.query.upper():
            return [
                ("Column", None),
                ("Type", None),
                ("Null", None),
                ("Key", None),
                ("Default", None),
                ("Semantic Type", None),
            ]
        elif "VERSION()" in self.query.upper():
            return [("version()", None)]
        elif "TQL" in self.query.upper():
            return [("ts", None), ("host", None), ("value", None)]
        elif "EXPLAIN" in self.query.upper():
            return [("plan", None)]
        elif "ALIGN" in self.query.upper():
            return [("ts", None), ("host", None), ("avg_cpu", None)]
        elif "GREPTIME_PRIVATE.PIPELINES" in self.query.upper():
            return [("name", None), ("pipeline", None), ("version", None)]
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


class MockConnectionPool:
    """Mock for MySQL connection pool"""

    def __init__(self, *args, **kwargs):
        pass

    def get_connection(self):
        return MockConnection()


class MockMySQLModule:
    """Mock for entire mysql.connector module"""

    @staticmethod
    def connect(*args, **kwargs):
        return MockConnection(*args, **kwargs)

    class Error(Exception):
        """Mock MySQL error class"""

        pass


class MockPoolingModule:
    """Mock for mysql.connector.pooling module"""

    MySQLConnectionPool = MockConnectionPool


def pytest_configure(config):
    """
    Called at the start of the pytest session, before tests are collected.
    This is where we apply our global patches before any imports happen.
    """
    # Create and store original modules if they exist
    original_mysql = sys.modules.get("mysql.connector")
    original_pooling = sys.modules.get("mysql.connector.pooling")

    # Create mock MySQL modules
    sys.modules["mysql.connector"] = MockMySQLModule
    sys.modules["mysql.connector.pooling"] = MockPoolingModule

    # Store the original function for later import and patching
    config._mysql_original = original_mysql
    config._pooling_original = original_pooling


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Restore original modules after all tests are done"""
    if hasattr(session.config, "_mysql_original") and session.config._mysql_original:
        sys.modules["mysql.connector"] = session.config._mysql_original
    if (
        hasattr(session.config, "_pooling_original")
        and session.config._pooling_original
    ):
        sys.modules["mysql.connector.pooling"] = session.config._pooling_original
