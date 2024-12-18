import logging
from typing import Any, Optional
import mcp.types as types
from datetime import datetime
import pandas as pd
import numpy as np
from .memo_manager import MemoManager
from .database import MotherDuckDatabase

logger = logging.getLogger('mcp_motherduck_server.tools')

class ToolManager:
    def __init__(self, db: MotherDuckDatabase, memo_manager: MemoManager):
        self.db = db
        self.memo_manager = memo_manager
        logger.info("ToolManager initialized")

    def get_available_tools(self) -> list[types.Tool]:
        """Return list of available tools."""
        logger.debug("Retrieving available tools")
        tools = [
            types.Tool(
                name="list-tables",
                description=(
                    "Lists all available tables in the MotherDuck database with their full names (database.schema.table). "
                    "Uses DuckDB syntax. Optionally filter tables by database name."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Optional database name to filter tables (e.g., 'compound_pipeline')"
                        }
                    },
                },
            ),
            types.Tool(
                name="describe-table",
                description=(
                    "Get detailed information about a specific table's structure using DuckDB syntax. "
                    "Shows column names, types, nullability, and default values. "
                    "Provide the full table name in format: database.schema.table "
                    "(e.g., 'compound_pipeline.clinicaltrials.investigationalagent')"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Full table name in DuckDB format (e.g., 'compound_pipeline.clinicaltrials.investigationalagent')"
                        },
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="query",
                description=(
                    "Execute a SQL query using DuckDB syntax."
                    "Use fully qualified table names (database.schema.table). "
                    "Example: SELECT * FROM compound_pipeline.clinicaltrials.investigationalagent LIMIT 5"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL query in DuckDB syntax with fully qualified table names"
                        },
                    },
                    "required": ["sql"],
                },
            ),
            types.Tool(
                name="append-insight",
                description=(
                    "Record an insight or observation about the data analysis. "
                    "Use this to document important findings during analysis."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "insight": {
                            "type": "string",
                            "description": "The insight to record about the data analysis"
                        },
                    },
                    "required": ["insight"],
                },
            ),
        ]
        logger.debug(f"Retrieved {len(tools)} available tools")
        return tools

    async def execute_tool(self, name: str, arguments: dict[str, Any] | None) -> list[types.TextContent]:
        """Execute a tool with given arguments."""
        logger.info(f"Executing tool: {name} with arguments: {arguments}")

        try:
            if name == "list-tables":
                database_filter = arguments.get('database') if arguments else None
                
                query = """
                SELECT 
                    table_catalog as database_name,
                    table_schema as schema_name,
                    table_name,
                    concat(table_catalog, '.', table_schema, '.', table_name) as full_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                """
                
                if database_filter:
                    query += f" AND table_catalog = '{database_filter}'"
                
                query += " ORDER BY table_catalog, table_schema, table_name;"
                
                results = self.db.execute_query(query)
                
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

            elif name == "describe-table":
                if not arguments or "table_name" not in arguments:
                    raise ValueError("Table name is required")
                
                table_name = arguments["table_name"]
                
                # Split the full table name into parts
                parts = table_name.split('.')
                if len(parts) != 3:
                    raise ValueError("Table name must be in format: database.schema.table")
                
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

            elif name == "query":
                if not arguments or "sql" not in arguments:
                    raise ValueError("SQL query is required")
                
                sql = arguments["sql"]
                results = self.db.execute_query(sql)
                
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

            elif name == "append-insight":
                if not arguments or "insight" not in arguments:
                    raise ValueError("Insight text is required")
                
                insight = arguments["insight"]
                self.memo_manager.add_insights(insight)
                return [types.TextContent(
                    type="text",
                    text="Insight recorded successfully"
                )]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise

