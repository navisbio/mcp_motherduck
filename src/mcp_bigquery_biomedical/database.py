import logging
import os
from typing import Any, Optional
import duckdb
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('mcp_motherduck_server.database')

class MotherDuckDatabase:
    def __init__(self):
        logger.info("Initializing MotherDuck database connection")
        self.conn = self._initialize_motherduck_connection()
        logger.info("MotherDuck database initialization complete")

    def _initialize_motherduck_connection(self) -> duckdb.DuckDBPyConnection:
        """Initializes the MotherDuck connection."""
        logger.debug("Initializing MotherDuck connection")
        
        # Get the MotherDuck token from environment variables
        motherduck_token = os.environ.get('MOTHERDUCK_TOKEN')
        if not motherduck_token:
            raise ValueError("MOTHERDUCK_TOKEN environment variable is required")

        try:
            # Connect to MotherDuck with token set during initialization
            conn = duckdb.connect(f"md:", config={
                'motherduck_token': motherduck_token
            }, read_only=True)
            
            logger.info("MotherDuck connection initialized")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to MotherDuck: {str(e)}", exc_info=True)
            raise

    def execute_query(self, query: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries"""
        logger.debug(f"Executing query: {query}")
        
        try:
            if params:
                logger.debug(f"Query parameters: {params}")
                # Replace parameters in query using string formatting
                # Note: This is a simplified approach - in production you'd want to use proper parameter binding
                for key, value in params.items():
                    query = query.replace(f"@{key}", f"'{value}'")

            # Execute the query
            result = self.conn.execute(query).fetchdf()
            
            # Convert DataFrame to list of dictionaries
            rows = result.to_dict('records')
            
            logger.debug(f"Query returned {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"MotherDuck error executing query: {str(e)}", exc_info=True)
            raise RuntimeError(f"MotherDuck error: {str(e)}")
