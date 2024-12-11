import logging
from typing import Any
import json
import mcp.types as types
from .database import MotherDuckDatabase
from .memo_manager import MemoManager

logger = logging.getLogger('mcp_motherduck_server.tools')

class ToolManager:
    def __init__(self, db: MotherDuckDatabase, memo_manager: MemoManager):
        self.db = db
        self.memo_manager = memo_manager
        logger.info("ToolManager initialized")

    def get_available_tools(self) -> list[types.Tool]:
        """Return list of available tools"""
        logger.debug("Retrieving available tools")
        tools = [
            types.Tool(
                name="read-query",
                description="Execute a SELECT query on the MotherDuck database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SELECT SQL query to execute"},
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="list-tables",
                description="List all tables in the MotherDuck database",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="describe-table",
                description="Get the schema information for a specific table in MotherDuck",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Name of the table to describe"},
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="append-analysis",
                description="Add findings and insights related to the analysis question to the memo",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "Analysis finding about patterns or trends"},
                    },
                    "required": ["finding"],
                },
            ),
        ]
        logger.debug(f"Retrieved {len(tools)} available tools")
        return tools

    async def execute_tool(self, name: str, arguments: dict[str, Any] | None) -> list[types.TextContent]:
        """Execute a tool with given arguments"""
        logger.info(f"Executing tool: {name} with arguments: {arguments}")

        try:
            if name not in {tool.name for tool in self.get_available_tools()}:
                logger.error(f"Unknown tool requested: {name}")
                raise ValueError(f"Unknown tool: {name}")

            if not arguments and name != "list-tables":
                logger.error("Missing required arguments for tool execution")
                raise ValueError("Missing required arguments")

            elif name == "list-tables":
                logger.debug("Executing list-tables query")
                results, columns = self.db.execute_query(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_catalog = '{self.db.database}'
                    AND table_schema = 'main'
                    ORDER BY table_name;
                """)
                table_names = [row[0] for row in results]
                logger.info(f"Retrieved {len(table_names)} tables from database {self.db.database}")
                return [types.TextContent(type="text", text="\n".join(table_names))]

            elif name == "describe-table":
                if "table_name" not in arguments:
                    logger.error("Missing table_name argument for describe-table")
                    raise ValueError("Missing table_name argument")

                table_name = arguments["table_name"]
                fully_qualified_table = f"{self.db.database}.{table_name}"
                logger.debug(f"Describing table: {fully_qualified_table}")
                
                # Using DuckDB's information_schema to get column information
                results, columns = self.db.execute_query(f"""
                    SELECT 
                        column_name,
                        data_type,
                        CASE is_nullable 
                            WHEN 'YES' THEN 'NULLABLE'
                            ELSE 'NOT NULL'
                        END as nullable
                    FROM information_schema.columns 
                    WHERE table_catalog = '{self.db.database}'
                    AND table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                
                if not results:
                    logger.warning(f"Table {fully_qualified_table} does not exist")
                    return [types.TextContent(type="text", text=f"Table '{fully_qualified_table}' does not exist.")]
                    
                columns_info = "\n".join(f"{row[0]} | {row[1]} | {row[2]}" for row in results)
                logger.info(f"Retrieved schema for table {fully_qualified_table}")
                return [types.TextContent(type="text", text=columns_info)]
            elif name == "read-query":
                query = arguments.get("query", "").strip()
                # Validate the query
                if not query.upper().startswith("SELECT"):
                    logger.error(f"Invalid query type attempted: {query[:50]}...")
                    raise ValueError("Only SELECT statements are allowed.")

                # Check for disallowed statements
                disallowed_keywords = ["ATTACH", "DETACH", "PRAGMA", "ALTER", "DROP", "CREATE", "INSERT", "UPDATE", "DELETE"]
                if any(keyword in query.upper() for keyword in disallowed_keywords):
                    logger.error("Query contains disallowed statements.")
                    raise ValueError("Only SELECT statements are allowed.")

                # Modify query to use fully qualified table names
                modified_query = f"USE {self.db.database}; {query}"
                logger.debug(f"Executing query: {modified_query}")
                results, columns = self.db.execute_query(modified_query)                
                
                if not results:
                    logger.info("Query returned no results.")
                    return [types.TextContent(type="text", text="No results found.")]
                else:
                    # Format the results as a list of dictionaries
                    rows = [dict(zip(columns, row)) for row in results]
                    formatted_results = json.dumps(rows, indent=2)
                    logger.info(f"Query returned {len(rows)} rows")
                    return [types.TextContent(type="text", text=formatted_results)]       
                
                     
            elif name == "append-analysis":
                if "finding" not in arguments:
                    logger.error("Missing finding argument for append-analysis")
                    raise ValueError("Missing finding argument")

                logger.debug(f"Adding analysis finding: {arguments['finding'][:50]}...")
                self.memo_manager.add_finding(arguments["finding"])
                logger.info("Analysis finding added successfully")
                return [types.TextContent(type="text", text="Analysis finding added")]

        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise