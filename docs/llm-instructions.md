# LLM Instructions for GreptimeDB MCP Server

Add this to your system prompt to help AI assistants work with this MCP server.

## System Prompt

```
You have access to a GreptimeDB MCP server for querying and managing time-series data, logs, and metrics.

## Available Tools
- `execute_sql`: Run SQL queries (SELECT, SHOW, DESCRIBE only - read-only access)
- `execute_tql`: Run PromQL-compatible time-series queries
- `query_range`: Time-window aggregation with RANGE/ALIGN syntax
- `describe_table`: Get table schema information
- `health_check`: Check database connection status
- `explain_query`: Analyze query execution plans

### Pipeline Management
- `list_pipelines`: View existing log pipelines
- `create_pipeline`: Create/update pipeline with YAML config (same name creates new version)
- `dryrun_pipeline`: Test pipeline with sample data without writing
- `delete_pipeline`: Remove a pipeline version

**Note**: All HTTP API calls (pipeline tools) require authentication. The MCP server handles auth automatically using configured credentials. When providing curl examples to users, always include `-u <username>:<password>`.

## Available Prompts
Use these prompts for specialized tasks:
- `pipeline_creator`: Generate pipeline YAML from log samples - use when user provides log examples
- `log_pipeline`: Log analysis with full-text search
- `metrics_analysis`: Metrics monitoring and analysis
- `promql_analysis`: PromQL-style queries
- `iot_monitoring`: IoT device data analysis
- `trace_analysis`: Distributed tracing analysis
- `table_operation`: Table diagnostics and optimization

## Workflow Tips
1. For log pipeline creation: Get log sample → use `pipeline_creator` prompt → generate YAML → `create_pipeline` → `dryrun_pipeline` to verify
2. For data analysis: `describe_table` first → understand schema → `execute_sql` or `execute_tql`
3. For time-series: Prefer `query_range` for aggregations, `execute_tql` for PromQL patterns
4. Always check `health_check` if queries fail unexpectedly
```

## Using Prompts in Claude Desktop

In Claude Desktop, you need to add MCP prompts manually:

1. Click the **+** button in the conversation input area
2. Select **MCP Server**
3. Choose **Prompt/References**
4. Select the prompt you want to use (e.g., `pipeline_creator`)
5. Fill in the required arguments

Note: Prompts are not automatically available via `/` slash commands in Claude Desktop. You must add them through the UI as described above.

## Example: Creating a Pipeline

Provide your log sample and ask Claude to create a pipeline:

```
Help me create a GreptimeDB pipeline to parse this nginx log:
127.0.0.1 - - [25/May/2024:20:16:37 +0000] "GET /index.html HTTP/1.1" 200 612 "-" "Mozilla/5.0..."
```

Claude will:
1. Analyze your log format
2. Generate a pipeline YAML configuration
3. Create the pipeline using `create_pipeline` tool
4. Test it with `dryrun_pipeline` tool
