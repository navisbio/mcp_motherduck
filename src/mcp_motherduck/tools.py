import logging
from typing import Any, Optional
import mcp.types as types
import pandas as pd
from .database import MotherDuckDatabase

logger = logging.getLogger('mcp_motherduck_server.tools')

class ToolManager:
    def __init__(self, db: MotherDuckDatabase):
        self.db = db
        logger.info("ToolManager initialized")

    def get_available_tools(self) -> list[types.Tool]:
        """Return list of available tools."""
        logger.debug("Retrieving available tools")
        tools = [
            types.Tool(
                name="motherduck-list-tables",
                description=(
                    "Lists available tables in the MotherDuck database using DuckDB syntax. "
                    "Use this tool first to discover available tables. "
                    "Optional: Filter by database name using the 'database' parameter."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name to filter tables (e.g., 'compound_pipeline')"
                        }
                    },
                },
            ),
            types.Tool(
                name="motherduck-describe-table",
                description=(
                    "Shows detailed table structure using DuckDB syntax. "
                    "Use after exploring available tables with motherduck-list-tables. "
                    "Requires full table name in format: database.schema.table"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Full table name (e.g., 'compound_pipeline.oncology_all.genetarget')"
                        },
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="motherduck-query",
                description=(
                    "Executes read-only SQL queries using DuckDB syntax. "
                    "Use after exploring tables with motherduck-list-tables and motherduck-describe-table. "
                    "Always use fully qualified table names (database.schema.table). "
                    "Example: SELECT * FROM compound_pipeline.oncology_all.genetarget LIMIT 5"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL query with fully qualified table names"
                        },
                    },
                    "required": ["sql"],
                },
            ),
        ]
        logger.debug(f"Retrieved {len(tools)} available tools")
        return tools

    async def execute_tool(self, name: str, arguments: dict[str, Any] | None) -> list[types.TextContent]:
        """Execute a tool with given arguments."""
        logger.info(f"Executing tool: {name} with arguments: {arguments}")

        try:
            if name == "motherduck-list-tables":
                database_filter = arguments.get('database') if arguments else None
                
                # Build the base query
                query = """
                SELECT 
                    table_catalog as database_name,
                    table_schema as schema_name,
                    table_name,
                    concat(table_catalog, '.', table_schema, '.', table_name) as full_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                """
                
                # Add database filter from arguments
                if database_filter:
                    query += f" AND table_catalog = '{database_filter}'"
                # Add allowed datasets filter if configured
                elif self.db.allowed_datasets:
                    conditions = []
                    for db, schema in self.db.allowed_datasets:
                        if schema:
                            conditions.append(f"(table_catalog = '{db}' AND table_schema = '{schema}')")
                        else:
                            conditions.append(f"table_catalog = '{db}'")
                    query += f" AND ({' OR '.join(conditions)})"
                
                query += " ORDER BY table_catalog, table_schema, table_name;"
                
                results = self.db.execute_query(query)
                
                if isinstance(results, dict) and 'error' in results:
                    return [types.TextContent(
                        type="text",
                        text=f"Error listing tables: {results['error']}"
                    )]
                
                if not results:
                    return [types.TextContent(
                        type="text",
                        text="No tables found" + (f" in database '{database_filter}'" if database_filter else "")
                    )]
                
                # Format the output
                output_lines = ["Available tables:"]
                for row in results:
                    output_lines.append(f"- {row['full_name']}")
                
                return [types.TextContent(
                    type="text",
                    text="\n".join(output_lines)
                )]

            elif name == "motherduck-describe-table":
                if not arguments or "table_name" not in arguments:
                    return [types.TextContent(
                        type="text",
                        text="Error: Table name is required"
                    )]
                
                table_name = arguments["table_name"]
                
                # Split the full table name into parts
                parts = table_name.split('.')
                if len(parts) != 3:
                    return [types.TextContent(
                        type="text",
                        text="Error: Table name must be in format: database.schema.table"
                    )]
                
                database, schema, table = parts
                
                query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_catalog = '{database}'
                AND table_schema = '{schema}'
                AND table_name = '{table}'
                ORDER BY ordinal_position;
                """
                results = self.db.execute_query(query)
                
                if isinstance(results, dict) and 'error' in results:
                    return [types.TextContent(
                        type="text",
                        text=f"Error describing table: {results['error']}"
                    )]
                
                if not results:
                    return [types.TextContent(
                        type="text",
                        text=f"No columns found for table '{table_name}'"
                    )]
                
                description = [
                    f"Table: {table_name}",
                    "Columns:",
                    *[f"- {row['column_name']} ({row['data_type']}, "
                      f"{'NULL' if row['is_nullable'] == 'YES' else 'NOT NULL'}"
                      f"{', DEFAULT: ' + str(row['column_default']) if row['column_default'] else ''})"
                      for row in results]
                ]
                return [types.TextContent(
                    type="text",
                    text="\n".join(description)
                )]

            elif name == "motherduck-query":
                if not arguments or "sql" not in arguments:
                    return [types.TextContent(
                        type="text",
                        text="Error: SQL query is required"
                    )]
                
                sql = arguments["sql"]
                results = self.db.execute_query(sql)
                
                if isinstance(results, dict) and 'error' in results:
                    return [types.TextContent(
                        type="text",
                        text=f"Error executing query: {results['error']}"
                    )]
                
                if not results:
                    return [types.TextContent(
                        type="text",
                        text="Query returned no results"
                    )]
                
                # Convert results to a formatted string
                df = pd.DataFrame(results)
                return [types.TextContent(
                    type="text",
                    text=df.to_string()
                )]

            else:
                return [types.TextContent(
                    type="text",
                    text=f"Error: Unknown tool '{name}'"
                )]

        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            return [types.TextContent(
                type="text",
                text=f"Error executing tool: {str(e)}"
            )]

