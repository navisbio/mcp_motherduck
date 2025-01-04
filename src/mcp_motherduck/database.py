import logging
import os
from typing import Any, Optional
import duckdb
from dotenv import load_dotenv
import time
from contextlib import contextmanager
from threading import Lock

load_dotenv()

logger = logging.getLogger('mcp_motherduck_server.database')

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors"""
    pass

class MotherDuckDatabase:
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0, query_timeout: int = 30):
        """Initialize the MotherDuck database connection manager.
        
        Args:
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retry attempts in seconds
            query_timeout: Query execution timeout in seconds
        """
        logger.info("Initializing MotherDuck database connection")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.query_timeout = query_timeout
        self._conn = None
        self._conn_lock = Lock()
        self._initialize_connection()
        logger.info("MotherDuck database initialization complete")

    def _initialize_connection(self) -> None:
        """Initialize the database connection with retries."""
        for attempt in range(self.max_retries):
            try:
                self._conn = self._create_connection()
                self._test_connection()
                return
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error("Failed to initialize database connection after %d attempts", self.max_retries)
                    raise DatabaseConnectionError(f"Failed to initialize connection: {str(e)}")
                logger.warning("Connection attempt %d failed, retrying in %.1f seconds", attempt + 1, self.retry_delay)
                time.sleep(self.retry_delay)

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new database connection."""
        motherduck_token = os.environ.get('MOTHERDUCK_TOKEN')
        if not motherduck_token:
            raise ValueError("MOTHERDUCK_TOKEN environment variable is required")

        try:
            conn = duckdb.connect("md:", config={
                'motherduck_token': motherduck_token,
                'custom_user_agent': 'mcp_motherduck_server',
            }, read_only=True)
            
            logger.info("Created new MotherDuck connection")
            return conn
        except Exception as e:
            logger.error("Error creating MotherDuck connection: %s", str(e))
            raise

    def _test_connection(self) -> bool:
        """Test if the connection is alive and working."""
        try:
            self._conn.execute("SELECT 1").fetchone()
            return True
        except Exception as e:
            logger.error("Connection test failed: %s", str(e))
            return False

    @contextmanager
    def get_connection(self):
        """Get a database connection with automatic reconnection if needed."""
        with self._conn_lock:
            if not self._conn or not self._test_connection():
                logger.info("Connection dead or missing, reconnecting...")
                self._initialize_connection()
            
            try:
                yield self._conn
            except Exception as e:
                logger.error("Error during connection usage: %s", str(e))
                self._conn = None  # Force reconnection next time
                raise

    def execute_query(self, query: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: The SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            List of dictionaries containing the query results
            
        Raises:
            DatabaseConnectionError: If connection fails
            RuntimeError: If query execution fails
            ValueError: If query timeout occurs
        """
        logger.debug("Executing query: %s", query)
        
        if params:
            # Use DuckDB's built-in parameter binding
            bound_params = {k: v for k, v in params.items()}
            logger.debug("Query parameters: %s", bound_params)
        else:
            bound_params = {}

        with self.get_connection() as conn:
            try:
                # Set query timeout
                conn.execute(f"SET timeout_duration='{self.query_timeout}s'")
                
                # Execute the query with proper parameter binding
                result = conn.execute(query, bound_params).fetchdf()
                
                # Convert DataFrame to list of dictionaries
                rows = result.to_dict('records')
                
                logger.debug("Query returned %d rows", len(rows))
                return rows
                
            except duckdb.TimeoutException:
                logger.error("Query timed out after %d seconds", self.query_timeout)
                raise ValueError(f"Query timed out after {self.query_timeout} seconds")
            except Exception as e:
                logger.error("Error executing query: %s", str(e))
                raise RuntimeError(f"Query execution error: {str(e)}")

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error("Error closing database connection: %s", str(e))
            finally:
                self._conn = None