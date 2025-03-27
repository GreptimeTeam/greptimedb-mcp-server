import re
import logging
import yaml
import os

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
        (r"\bDROP\s", "Forbided `DROP` operation"),
        (r"\bDELETE\s", "Forbided `DELETE` operation"),
        (r"\bREVOKE\s", "Forbided `REVOKE` operation"),
        (r"\bTRUNCATE\s", "Forbided `bTRUNCATE` operation"),
    ]

    for pattern, reason in dangerous_patterns:
        if re.search(pattern, normalized_query):
            logger.warning(f"Detected dangerous operation: '{query}' - {reason}")
            return True, reason

    return False, ""


def templates_loader() -> dict[str, dict[str, str]]:
    templates = {}
    template_dir = os.path.join(os.path.dirname(__file__), "..", "..", "templates")

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
