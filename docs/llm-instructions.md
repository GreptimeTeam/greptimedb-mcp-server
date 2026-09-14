# LLM Instructions for GreptimeDB MCP Server

Add the block below to your system prompt to help AI assistants work with this
MCP server.

## System Prompt

The tools are not listed here: the server advertises them over MCP with their
parameters and limits, and the [README](../README.md) has the table for
browsing. What follows is what MCP does not carry — which prompts exist, and
the order to do things in.

```
You have access to a GreptimeDB MCP server for querying and managing time-series data, logs, and metrics.

Read each tool's own description for its parameters, units and limits rather than assuming its shape.

**Note**: The MCP server handles HTTP API authentication automatically using configured credentials. When providing curl examples to users, include `-u <username>:<password>` only when GreptimeDB authentication is enabled.

## Available Prompts
Use these prompts for specialized tasks:
- `pipeline_creator`: Generate pipeline YAML from log samples - use when user provides log examples
- `log_pipeline`: Query and aggregate an existing log table
- `metrics_analysis`: SQL and RANGE analysis for existing time-series tables
- `promql_analysis`: TQL/PromQL expression help only
- `trace_analysis`: Query one trace table or drill into trace latency/errors
- `table_operation`: Inspect table schema, regions, storage, and cluster metadata
- `schema_design_advisor`: Design schema, primary key, indexes, append mode, and partitioning
- `observability_correlation`: Pivot across already identified metrics, logs, and traces
- `ingestion_troubleshooting`: Debug ingestion, schema, timestamp, and pipeline write issues
- `query_performance_tuning`: Analyze slow SQL, TQL, and RANGE queries from execution plans

## Workflow Tips
1. For log pipeline creation: Get log sample → use `pipeline_creator` prompt → generate YAML → `dryrun_pipeline` to verify → `create_pipeline`
2. For dashboard creation: Prepare Perses JSON definition → `create_dashboard` → verify with `list_dashboards`
3. For data analysis: identify the table first — `search_table_semantics` with concept words when its name is unknown — then `describe_table` for schema, semantics and samples, then `execute_sql` or `execute_tql`
4. For service topology and entity relationships: `query_semantic_graph`, starting with `view=summary` when the types in the graph are unknown
5. For time-series: Prefer `query_range` for aggregations, `execute_tql` for PromQL patterns
6. For schema design: collect workload, cardinality, and query patterns before proposing primary keys or indexes
7. If a query fails with a connection error, check `health_check`
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
3. Test it with `dryrun_pipeline` tool
4. Create the pipeline using `create_pipeline` tool
