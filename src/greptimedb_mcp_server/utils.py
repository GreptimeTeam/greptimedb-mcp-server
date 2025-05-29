import re
import logging
import yaml
import os

logger = logging.getLogger("greptimedb_mcp_server")


def security_gate(query: str) -> tuple[bool, str]:
    """
    Simple security check for SQL queries.
    Args:
        query: The SQL query to check
    Returns:
        tuple: A boolean indicating if the query is dangerous, and a reason message
    """
    if not query or not query.strip():
        return True, "Empty query not allowed"

    # Remove comments and normalize whitespace
    clean_query = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)  # Remove /* */
    clean_query = re.sub(r"--.*", "", clean_query)  # Remove --
    clean_query = re.sub(r"\s+", " ", clean_query).strip().upper()  # Normalize spaces

    # Check for dangerous patterns
    dangerous_patterns = [
        (r"\bDROP\b", "Forbided `DROP` operation"),
        (r"\bDELETE\b", "Forbided `DELETE` operation"),
        (r"\bREVOKE\b", "Forbided `REVOKE` operation"),
        (r"\bTRUNCATE\b", "Forbided `TRUNCATE` operation"),
        (r"\bUPDATE\b", "Forbided `UPDATE` operation"),
        (r"\bINSERT\b", "Forbided `INSERT` operation"),
        (r"\bALTER\b", "Forbided `ALTER` operation"),
        (r"\bCREATE\b", "Forbided `CREATE` operation"),
        (r"\bGRANT\b", "Forbided `GRANT` operation"),
        (r";\s*\w+", "Forbided multiple statements"),
    ]

    for pattern, reason in dangerous_patterns:
        if re.search(pattern, clean_query):
            logger.warning(f"Dangerous pattern detected: {query[:50]}...")
            return True, reason

    return False, ""


def templates_loader() -> dict[str, dict[str, str]]:
    templates = {}
    template_dir = os.path.join(os.path.dirname(__file__), "templates")

    for category in os.listdir(template_dir):
        category_path = os.path.join(template_dir, category)
        if os.path.isdir(category_path):
            # Load config
            with open(os.path.join(category_path, "config.yaml"), "r") as f:
                config = yaml.safe_load(f)

            # Load template
            with open(os.path.join(category_path, "template.md"), "r") as f:
                template = f.read()

            templates[category] = {"config": config, "template": template}

    return templates
