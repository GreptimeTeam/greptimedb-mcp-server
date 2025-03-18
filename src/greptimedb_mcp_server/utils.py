import re
import logging

logger = logging.getLogger("greptimedb_mcp_server")

def security_gate(query: str) -> tuple[bool, str]:
    """
    Check if a SQL query is dangerous and should be blocked.

    Args:
        query: The SQL query to check

    Returns:
        tuple: A boolean indicating if the query is dangerous, and a reason message
    """
    # format query to uppercase and remove leading/trailing whitespace
    normalized_query = query.strip().upper()

    # Define dangerous patterns
    dangerous_patterns = [
        (r'\bDROP\s', "Forbided `DROP` operation"),
        (r'\bDELETE\s', "Forbided `DELETE` operation"),
        (r'\bREVOKE\s', "Forbided `REVOKE` operation"),
        (r'\bTRUNCATE\s', "Forbided `bTRUNCATE` operation"),
    ]

    for pattern, reason in dangerous_patterns:
        if re.search(pattern, normalized_query):
            logger.warning(f"Detected dangerous operation: '{query}' - {reason}")
            return True, reason

    return False, ""
