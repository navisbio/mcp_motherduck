import logging
from mcp.server import Server, NotificationOptions, RequestContext
from mcp.server.models import InitializationOptions
import mcp.server.stdio
from .database import MotherDuckDatabase
from .handlers import MCPHandlers
from mcp.types import LoggingLevel, EmptyResult
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('mcp_motherduck_server')
logger.setLevel(logging.DEBUG)

class MotherDuckServer(Server):
    def __init__(self):
        super().__init__("motherduck-manager")
        self.db = MotherDuckDatabase()
        
        # Load the schema resource
        # Update the path if necessary
        schema_path = Path(__file__).parent / "resources" / "database_schema.json"
        if schema_path.exists():
            with open(schema_path) as f:
                self.schema = json.load(f)
        else:
            logger.warning("Database schema file not found.")
            self.schema = {}
        
        # Pass schema to handlers
        self.handlers = MCPHandlers(self.db, self.schema)
        self._register_handlers()
        
        # Set up logging handler that sends to MCP client
        self.log_handler = MCPLogHandler(self)
        logger.addHandler(self.log_handler)

    def _register_handlers(self):
        @self.list_resources()
        async def handle_list_resources():
            return await self.handlers.handle_list_resources()

        @self.read_resource()
        async def handle_read_resource(uri):
            return await self.handlers.handle_read_resource(uri)

        @self.list_tools()
        async def handle_list_tools():
            return await self.handlers.handle_list_tools()

        @self.call_tool()
        async def handle_call_tool(name, arguments):
            return await self.handlers.handle_call_tool(name, arguments)

        @self.set_logging_level()
        async def handle_set_logging_level(level: LoggingLevel) -> EmptyResult:
            """Handle requests to change the logging level"""
            logger.info(f"Setting logging level to {level}")
            logging.getLogger('mcp_motherduck_server').setLevel(level.upper())
            return EmptyResult()

class MCPLogHandler(logging.Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server

    def emit(self, record):
        try:
            # Only emit logs when we have an active session
            if (hasattr(self.server, 'request_context') and 
                self.server.request_context and 
                hasattr(self.server.request_context, 'session')):
                msg = self.format(record)
                self.server.request_context.session.send_log_message(
                    level=record.levelname.lower(),
                    data=msg
                )
        except Exception:
            # Silently ignore logging errors
            pass
async def main():
    try:
        server = MotherDuckServer()
        
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            logger.info("MotherDuck MCP Server running with stdio transport")
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="motherduck",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                )
            )
    except Exception as e:
        logger.error(f"Server error: {str(e)}", exc_info=True)
        raise