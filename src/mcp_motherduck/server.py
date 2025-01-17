import logging
from mcp.server import Server, NotificationOptions, RequestContext
from mcp.server.models import InitializationOptions
import mcp.server.stdio
from .database import MotherDuckDatabase
from .handlers import MCPHandlers
from mcp.types import LoggingLevel, EmptyResult
import json
from pathlib import Path

logger = logging.getLogger('mcp_motherduck_server')
logger.setLevel(logging.DEBUG)

class MotherDuckServer(Server):
    def __init__(self):
        super().__init__("motherduck")
        self.db = MotherDuckDatabase()
        
        try:
            # Load the schema resource
            schema_path = Path(__file__).parent / "resources" / "database_schema.json"
            logger.debug(f"Loading schema from: {schema_path}")
            
            if not schema_path.exists():
                logger.warning("Schema file not found, using empty schema")
                self.schema = {}
            else:
                with open(schema_path) as f:
                    self.schema = json.load(f)
                    logger.debug("Schema loaded successfully")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing schema file: {e}")
            logger.warning("Using empty schema due to parsing error")
            self.schema = {}
        except Exception as e:
            logger.error(f"Unexpected error loading schema: {e}")
            logger.warning("Using empty schema due to error")
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

        @self.list_prompts()
        async def handle_list_prompts():
            return await self.handlers.handle_list_prompts()

        @self.get_prompt()
        async def handle_get_prompt(name, arguments):
            return await self.handlers.handle_get_prompt(name, arguments)

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
            
            # Send confirmation through the session
            if hasattr(self, 'request_context') and self.request_context:
                await self.request_context.session.send_log_message(
                    level="info",
                    data=f"Log level set to {level}"
                )
            
            return EmptyResult()

    async def shutdown(self):
        """Clean up resources when shutting down the server."""
        logger.info("Shutting down MotherDuck server")
        try:
            if hasattr(self, 'db'):
                self.db.close()
            if hasattr(self, 'log_handler'):
                logger.removeHandler(self.log_handler)
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")
        finally:
            await super().shutdown()

class MCPLogHandler(logging.Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server

    def emit(self, record):
        try:
            if hasattr(self.server, 'request_context') and self.server.request_context:
                msg = self.format(record)
                self.server.request_context.session.send_log_message(
                    level=record.levelname.lower(),
                    data=msg
                )
        except Exception:
            self.handleError(record)

async def main():
    try:
        server = MotherDuckServer()
        
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="aact",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    except Exception as e:
        logger.error(f"Server error: {str(e)}", exc_info=True)
        raise
