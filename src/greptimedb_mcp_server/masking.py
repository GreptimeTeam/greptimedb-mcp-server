"""Data masking module for sensitive information protection."""

# Default sensitive column name patterns (case-insensitive, partial match)
DEFAULT_SENSITIVE_PATTERNS = [
    # Authentication credentials
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
    # Financial information
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
    # Personal privacy
    "ssn",
    "social_security",
    "id_card",
    "idcard",
    "passport",
]

MASK_PLACEHOLDER = "******"


def is_sensitive_column(column_name: str, patterns: list[str]) -> bool:
    """
    Check if a column name matches any sensitive pattern.

    Args:
        column_name: The column name to check
        patterns: List of sensitive patterns to match against

    Returns:
        True if the column name contains any sensitive pattern
    """
    if not column_name:
        return False

    column_lower = column_name.lower()
    for pattern in patterns:
        if pattern.lower() in column_lower:
            return True
    return False


def mask_rows(
    columns: list[str],
    rows: list[tuple],
    patterns: list[str] | None = None,
) -> list[tuple]:
    """
    Mask sensitive column values in query results.

    Args:
        columns: List of column names
        rows: List of row tuples from query results
        patterns: List of sensitive patterns (uses DEFAULT_SENSITIVE_PATTERNS if None)

    Returns:
        List of row tuples with sensitive values masked
    """
    if not columns or not rows:
        return rows

    if patterns is None:
        patterns = DEFAULT_SENSITIVE_PATTERNS

    # Find indices of sensitive columns
    sensitive_indices = set()
    for i, col in enumerate(columns):
        if is_sensitive_column(col, patterns):
            sensitive_indices.add(i)

    # No sensitive columns found
    if not sensitive_indices:
        return rows

    # Mask sensitive values
    masked_rows = []
    for row in rows:
        masked_row = tuple(
            MASK_PLACEHOLDER if i in sensitive_indices and val is not None else val
            for i, val in enumerate(row)
        )
        masked_rows.append(masked_row)

    return masked_rows
