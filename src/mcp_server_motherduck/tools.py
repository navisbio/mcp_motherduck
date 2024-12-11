import logging
from typing import Any
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
                name="append-landscape",
                description="Add findings and insights related to the analysis question to the memo",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "Analysis finding about trial patterns or trends"},
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

            if name == "list-tables":
                logger.debug("Executing list-tables query")
                results = self.db.execute_query("""
                    SELECT name 
                    FROM sqlite_master 
                    WHERE type='table' 
                    ORDER BY name;
                """)
                table_names = [row[0] for row in results]
                logger.info(f"Retrieved {len(table_names)} tables")
                return [types.TextContent(type="text", text="\n".join(table_names))]

            elif name == "describe-table":
                if "table_name" not in arguments:
                    logger.error("Missing table_name argument for describe-table")
                    raise ValueError("Missing table_name argument")
                
                table_name = arguments["table_name"]
                logger.debug(f"Describing table: {table_name}")
                results = self.db.execute_query(f"PRAGMA table_info('{table_name}');")
                columns_info = "\n".join(str(row) for row in results)
                logger.info(f"Retrieved schema for table {table_name}")
                return [types.TextContent(type="text", text=columns_info)]

            elif name == "read-query":
                query = arguments.get("query", "").strip()
                if not query.upper().startswith("SELECT"):
                    logger.error(f"Invalid query type attempted: {query[:50]}...")
                    raise ValueError("Only SELECT queries are allowed for read-query")
                
                logger.debug(f"Executing query: {query}")
                results = self.db.execute_query(query)
                rows = [str(row) for row in results]
                logger.info(f"Query returned {len(rows)} rows")
                return [types.TextContent(type="text", text="\n".join(rows))]

            elif name == "append-landscape":
                if "finding" not in arguments:
                    logger.error("Missing finding argument for append-landscape")
                    raise ValueError("Missing finding argument")
                
                logger.debug(f"Adding landscape finding: {arguments['finding'][:50]}...")
                self.memo_manager.add_landscape_finding(arguments["finding"])
                logger.info("Landscape finding added successfully")
                return [types.TextContent(type="text", text="Landscape finding added")]

        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise