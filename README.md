# MotherDuck MCP Server

## Overview
A Model Context Protocol (MCP) server implementation that provides read-only access to MotherDuck databases. Supports database and schema-level access control.

## Tools

- `motherduck-list-tables`
   - Lists available tables in the database
   - Optional filter by database name
   - Example: `{"database": "compound_pipeline"}`

- `motherduck-describe-table`
   - Shows table structure (columns, types, nullability)
   - Requires full table name: database.schema.table
   - Example: `{"table_name": "compound_pipeline.oncology_all.genetarget"}`

- `motherduck-query`
   - Executes read-only SQL queries
   - Requires fully qualified table names
   - Example: `{"sql": "SELECT * FROM compound_pipeline.oncology_all.genetarget LIMIT 5"}`

## Configuration

### Required Environment Variables
- `MOTHERDUCK_TOKEN`: Your MotherDuck authentication token

### Optional Environment Variables
- `ALLOWED_DATASETS`: Restrict access to specific databases/schemas
  - Format: `database` or `database.schema` (comma-separated)
  - Examples:
    ```bash
    # Full database access
    ALLOWED_DATASETS=compound_pipeline

    # Single schema access
    ALLOWED_DATASETS=compound_pipeline.oncology_all

    # Multiple schemas
    ALLOWED_DATASETS=compound_pipeline.oncology_all,compound_pipeline.clinicaltrials
    ```

## Usage with Claude Desktop

Add to your `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "MOTHERDUCK-MCP": {
      "command": "python",
      "args": ["-m", "mcp_server_motherduck"],
      "env": {
        "MOTHERDUCK_TOKEN": "YOUR_TOKEN",
        "ALLOWED_DATASETS": "YOUR_ALLOWED_DATASETS"  // Optional
      }
    }
  }
}
```

## License
GNU General Public License v3.0 (GPL-3.0)
