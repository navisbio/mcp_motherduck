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
                    "Lists all available tables in the clinical trials database. "
                    "Use this to explore the database structure and available data tables."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="describe-table",
                description=(
                    "Get detailed information about a specific table's structure including column names and types. "
                    "Use this to understand what data is available in each table."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the table to describe"
                        },
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="query",
                description=(
                    "Execute a SQL query on the clinical trials database. "
                    "Use this to analyze clinical trial data and extract specific information."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "SQL query to execute"
                        },
                    },
                    "required": ["sql"],
                },
            ),
            types.Tool(
                name="append-insight",
                description=(
                    "Record an insight or observation about the clinical trials data. "
                    "Use this to document important findings during analysis."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "insight": {
                            "type": "string",
                            "description": "The insight to record"
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
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
                ORDER BY table_name;
                """
                results = self.db.execute_query(query)
                tables = [row['table_name'] for row in results]
                return [types.TextContent(
                    type="text",
                    text=f"Available tables:\n{', '.join(tables)}"
                )]

            elif name == "describe-table":
                if not arguments or "table_name" not in arguments:
                    raise ValueError("Table name is required")
                
                table_name = arguments["table_name"]
                query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                AND table_schema = 'main'
                ORDER BY ordinal_position;
                """
                results = self.db.execute_query(query)
                
                if not results:
                    return [types.TextContent(
                        type="text",
                        text=f"No columns found for table '{table_name}'"
                    )]
                
                description = [
                    f"Table: {table_name}\n",
                    "Columns:",
                    *[f"- {row['column_name']} ({row['data_type']}, {'NULL' if row['is_nullable'] == 'YES' else 'NOT NULL'})"
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

