import logging
import os
import duckdb
from contextlib import closing
from typing import Any

from dotenv import load_dotenv

load_dotenv()


logger = logging.getLogger('mcp_motherduck_server.database')

class MotherDuckDatabase:
    def __init__(self):
        logger.info("Initializing MotherDuck database connection")
        self.token = os.environ.get("MOTHERDUCK_TOKEN")
        self.database = os.environ.get('MOTHERDUCK_DATABASE')

        # if not self.token:
        #     logger.error("Missing MotherDuck token")
        #     raise ValueError("MOTHERDUCK_TOKEN environment variable must be set")

        # if not self.database:
        #     logger.error("Missing MotherDuck database name")
        #     raise ValueError("MOTHERDUCK_DATABASE environment variable must be set")

        # Construct the connection string with the database name
        self.connection_string = f"md:{self.database}?motherduck_token={self.token}"
        self._test_connection()
        logger.info("MotherDuck database initialization complete")

    def _test_connection(self):
        """Test connection to MotherDuck"""
        logger.debug("Testing database connection")
        try:
            with closing(self._get_connection()) as conn:
                conn.execute("SELECT 1").fetchone()
                logger.info(f"Connected to MotherDuck database: {self.database}")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}", exc_info=True)
            raise RuntimeError(f"Database connection failed: {str(e)}")

    def _get_connection(self):
        """Get a new database connection"""
        logger.debug(f"Creating new database connection to database: {self.database}")
        try:
            return duckdb.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Failed to create connection: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to create connection: {str(e)}")

    def execute_query(self, query: str, params: list[Any] | None = None) -> tuple[list[tuple[Any, ...]], list[str]]:
        """Execute a SQL query and return results and column names"""
        logger.debug(f"Executing query: {query}")
        try:
            with closing(self._get_connection()) as conn:
                cursor = conn.execute(query, params or [])
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                logger.debug(f"Query returned {len(results)} rows")
                return results, columns
        except Exception as e:
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise
