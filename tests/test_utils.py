import pytest
from greptimedb_mcp_server.utils import templates_loader


def test_templates_loader_basic():
    """Test that templates_loader can load existing templates"""
    # Call the function under test
    templates = templates_loader()

    # Basic validation that we got something back
    assert templates is not None

    # Check if templates is a dictionary
    assert isinstance(templates, dict), "Expected templates to be a dictionary"
    assert len(templates) > 0, "Expected templates dictionary to have items"

    # Check if the metrics_analysis template is in the dictionary
    assert "metrics_analysis" in templates, "metrics_analysis template not found"

    # Get the metrics_analysis template
    metrics_template = templates["metrics_analysis"]

    # Get the metrics_analysis template config
    config = metrics_template["config"]

    # Check that the config has the expected structure
    assert isinstance(config, dict), "Expected template to be a dictionary"
    assert "description" in config, "Template config missing 'description' field"
    assert "arguments" in config, "Template config missing 'arguments' field"
    assert "metadata" in config, "Template config missing 'metadata' field"

    # Check that the template has the expected arguments
    arguments = config["arguments"]
    assert isinstance(arguments, list), "Expected arguments to be a list"

    arg_names = [
        arg.get("name") for arg in arguments if isinstance(arg, dict) and "name" in arg
    ]
    expected_args = ["topic", "start_time", "end_time"]
    for arg in expected_args:
        assert (
            arg in arg_names
        ), f"Expected argument '{arg}' not found in metrics_analysis template"

    # Check template content
    tpl = metrics_template["template"]
    assert "{{ topic }}" in tpl
    assert "{{ start_time }}" in tpl
    assert "{{ end_time }}" in tpl
