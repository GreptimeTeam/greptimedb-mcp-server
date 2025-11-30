"""Tests for data masking functionality."""

import pytest

from greptimedb_mcp_server.masking import (
    DEFAULT_SENSITIVE_PATTERNS,
    MASK_PLACEHOLDER,
    is_sensitive_column,
    mask_rows,
)
from greptimedb_mcp_server.formatter import format_results


class TestIsSensitiveColumn:
    """Tests for is_sensitive_column function."""

    def test_exact_match(self):
        """Test exact pattern match."""
        assert is_sensitive_column("password", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("secret", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("token", DEFAULT_SENSITIVE_PATTERNS)

    def test_case_insensitive(self):
        """Test case-insensitive matching."""
        assert is_sensitive_column("PASSWORD", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("Password", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("SECRET", DEFAULT_SENSITIVE_PATTERNS)

    def test_partial_match(self):
        """Test partial pattern match (column name contains pattern)."""
        assert is_sensitive_column("user_password", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("password_hash", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("api_token", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("access_token_secret", DEFAULT_SENSITIVE_PATTERNS)

    def test_financial_patterns(self):
        """Test financial sensitive patterns."""
        assert is_sensitive_column("credit_card", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("creditcard_number", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("cvv", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("bank_account_id", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("iban_code", DEFAULT_SENSITIVE_PATTERNS)

    def test_privacy_patterns(self):
        """Test personal privacy patterns."""
        assert is_sensitive_column("ssn", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("social_security_number", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("passport_number", DEFAULT_SENSITIVE_PATTERNS)
        assert is_sensitive_column("id_card", DEFAULT_SENSITIVE_PATTERNS)

    def test_non_sensitive_columns(self):
        """Test non-sensitive columns are not matched."""
        assert not is_sensitive_column("id", DEFAULT_SENSITIVE_PATTERNS)
        assert not is_sensitive_column("name", DEFAULT_SENSITIVE_PATTERNS)
        assert not is_sensitive_column("email", DEFAULT_SENSITIVE_PATTERNS)
        assert not is_sensitive_column("created_at", DEFAULT_SENSITIVE_PATTERNS)
        assert not is_sensitive_column("user_id", DEFAULT_SENSITIVE_PATTERNS)

    def test_empty_column_name(self):
        """Test empty column name returns False."""
        assert not is_sensitive_column("", DEFAULT_SENSITIVE_PATTERNS)
        assert not is_sensitive_column(None, DEFAULT_SENSITIVE_PATTERNS)

    def test_custom_patterns(self):
        """Test custom patterns."""
        custom = ["phone", "address"]
        assert is_sensitive_column("phone_number", custom)
        assert is_sensitive_column("home_address", custom)
        assert not is_sensitive_column("password", custom)


class TestMaskRows:
    """Tests for mask_rows function."""

    def test_mask_sensitive_columns(self):
        """Test sensitive column values are masked."""
        columns = ["id", "name", "password"]
        rows = [(1, "Alice", "secret123"), (2, "Bob", "pass456")]

        masked = mask_rows(columns, rows)

        assert masked[0] == (1, "Alice", MASK_PLACEHOLDER)
        assert masked[1] == (2, "Bob", MASK_PLACEHOLDER)

    def test_preserve_non_sensitive(self):
        """Test non-sensitive columns are preserved."""
        columns = ["id", "name", "email"]
        rows = [(1, "Alice", "alice@example.com")]

        masked = mask_rows(columns, rows)

        assert masked[0] == (1, "Alice", "alice@example.com")

    def test_multiple_sensitive_columns(self):
        """Test multiple sensitive columns are masked."""
        columns = ["id", "password", "api_key", "token"]
        rows = [(1, "pass1", "key123", "tok456")]

        masked = mask_rows(columns, rows)

        assert masked[0] == (1, MASK_PLACEHOLDER, MASK_PLACEHOLDER, MASK_PLACEHOLDER)

    def test_null_values_not_masked(self):
        """Test NULL values are not masked."""
        columns = ["id", "password"]
        rows = [(1, None), (2, "secret")]

        masked = mask_rows(columns, rows)

        assert masked[0] == (1, None)
        assert masked[1] == (2, MASK_PLACEHOLDER)

    def test_empty_rows(self):
        """Test empty rows list."""
        columns = ["id", "password"]
        rows = []

        masked = mask_rows(columns, rows)

        assert masked == []

    def test_empty_columns(self):
        """Test empty columns list."""
        columns = []
        rows = [(1, 2, 3)]

        masked = mask_rows(columns, rows)

        assert masked == [(1, 2, 3)]

    def test_custom_patterns(self):
        """Test custom patterns extend default masking."""
        columns = ["id", "phone", "email"]
        rows = [(1, "123-456-7890", "test@example.com")]

        # Without custom patterns, phone is not masked
        masked_default = mask_rows(columns, rows, DEFAULT_SENSITIVE_PATTERNS)
        assert masked_default[0][1] == "123-456-7890"

        # With custom patterns, phone is masked
        custom = list(DEFAULT_SENSITIVE_PATTERNS) + ["phone"]
        masked_custom = mask_rows(columns, rows, custom)
        assert masked_custom[0][1] == MASK_PLACEHOLDER


class TestFormatResultsWithMasking:
    """Tests for format_results with masking integration."""

    def test_csv_format_with_masking(self):
        """Test CSV format applies masking."""
        columns = ["id", "password"]
        rows = [(1, "secret123")]

        result = format_results(columns, rows, "csv", mask_enabled=True)

        assert "id,password" in result
        assert "secret123" not in result
        assert MASK_PLACEHOLDER in result

    def test_json_format_with_masking(self):
        """Test JSON format applies masking."""
        columns = ["id", "api_key"]
        rows = [(1, "key123")]

        result = format_results(columns, rows, "json", mask_enabled=True)

        assert "key123" not in result
        assert MASK_PLACEHOLDER in result

    def test_markdown_format_with_masking(self):
        """Test Markdown format applies masking."""
        columns = ["id", "token"]
        rows = [(1, "tok456")]

        result = format_results(columns, rows, "markdown", mask_enabled=True)

        assert "tok456" not in result
        assert MASK_PLACEHOLDER in result

    def test_masking_disabled(self):
        """Test masking can be disabled."""
        columns = ["id", "password"]
        rows = [(1, "secret123")]

        result = format_results(columns, rows, "csv", mask_enabled=False)

        assert "secret123" in result
        assert MASK_PLACEHOLDER not in result

    def test_custom_patterns_in_format(self):
        """Test custom patterns in format_results."""
        columns = ["id", "custom_field"]
        rows = [(1, "sensitive_data")]

        # Without custom pattern
        result1 = format_results(columns, rows, "csv", mask_enabled=True)
        assert "sensitive_data" in result1

        # With custom pattern
        result2 = format_results(
            columns, rows, "csv", mask_enabled=True, mask_patterns=["custom_field"]
        )
        assert "sensitive_data" not in result2
        assert MASK_PLACEHOLDER in result2


class TestDefaultPatterns:
    """Tests for default sensitive patterns coverage."""

    def test_authentication_patterns(self):
        """Test authentication-related patterns."""
        auth_patterns = [
            "password",
            "passwd",
            "pwd",
            "secret",
            "token",
            "api_key",
            "apikey",
            "access_key",
            "private_key",
            "credential",
            "auth",
            "authorization",
        ]
        for pattern in auth_patterns:
            assert pattern in DEFAULT_SENSITIVE_PATTERNS, f"Missing pattern: {pattern}"

    def test_financial_patterns(self):
        """Test financial patterns."""
        financial_patterns = [
            "credit_card",
            "creditcard",
            "card_number",
            "cardnumber",
            "cvv",
            "cvc",
            "pin",
            "bank_account",
            "account_number",
            "iban",
            "swift",
        ]
        for pattern in financial_patterns:
            assert pattern in DEFAULT_SENSITIVE_PATTERNS, f"Missing pattern: {pattern}"

    def test_privacy_patterns(self):
        """Test privacy patterns."""
        privacy_patterns = [
            "ssn",
            "social_security",
            "id_card",
            "idcard",
            "passport",
        ]
        for pattern in privacy_patterns:
            assert pattern in DEFAULT_SENSITIVE_PATTERNS, f"Missing pattern: {pattern}"
