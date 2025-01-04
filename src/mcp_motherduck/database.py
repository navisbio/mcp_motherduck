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
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize the MotherDuck database connection manager.
        
        Args:
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retry attempts in seconds
        """
        logger.info("Initializing MotherDuck database connection")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._conn = None
        self._conn_lock = Lock()
        
        # Get allowed datasets from environment
        allowed_datasets = os.environ.get('ALLOWED_DATASETS', '').strip()
        # Parse into tuples of (database, schema) where schema is optional
        self.allowed_datasets = []
        if allowed_datasets:
            for dataset in allowed_datasets.split(','):
                parts = dataset.strip().split('.')
                if len(parts) == 1:
                    self.allowed_datasets.append((parts[0], None))
                    logger.info("Access restricted to database: %s", parts[0])
                elif len(parts) == 2:
                    self.allowed_datasets.append((parts[0], parts[1]))
                    logger.info("Access restricted to database.schema: %s.%s", parts[0], parts[1])
        else:
            logger.info("No dataset restrictions applied")
        
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

    def _validate_query(self, query: str) -> tuple[bool, str]:
        """Validate if the query only accesses allowed databases and schemas.
        
        Args:
            query: The SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.allowed_datasets:
            return True, ""

        # Convert query to lowercase for case-insensitive comparison
        query_lower = query.lower()
        
        # Allow access to system schemas
        if "information_schema." in query_lower or "pg_catalog." in query_lower:
            return True, ""

        # Extract all potential database references (assuming format: database.schema.table or database.schema)
        import re
        # Match patterns like database.schema.table or database.schema
        db_refs = re.findall(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)(?:\.([a-zA-Z0-9_]+))?', query_lower)
        
        if not db_refs:
            return True, ""  # No database references found
            
        # Check each database reference against allowed datasets
        unauthorized = []
        for db, schema, _ in db_refs:
            is_allowed = False
            for allowed_db, allowed_schema in self.allowed_datasets:
                if db == allowed_db.lower():
                    if allowed_schema is None or schema == allowed_schema.lower():
                        is_allowed = True
                        break
            if not is_allowed:
                if any(allowed_db.lower() == db for allowed_db, _ in self.allowed_datasets):
                    unauthorized.append(f"{db}.{schema}")
                else:
                    unauthorized.append(db)

        if unauthorized:
            return False, f"Access denied to: {', '.join(set(unauthorized))}"
            
        return True, ""

    def execute_query(self, query: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]] | dict[str, str]:
        """Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: The SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            Either a list of dictionaries containing the query results,
            or a dictionary with an 'error' key containing the error message
        """
        logger.debug("Executing query: %s", query)
        
        # Validate query against allowed datasets
        is_valid, error_msg = self._validate_query(query)
        if not is_valid:
            logger.error("Query validation failed: %s", error_msg)
            return {"error": error_msg}
        
        if params:
            # Use DuckDB's built-in parameter binding
            bound_params = {k: v for k, v in params.items()}
            logger.debug("Query parameters: %s", bound_params)
        else:
            bound_params = {}

        try:
            with self._conn_lock:
                if not self._conn or not self._test_connection():
                    logger.info("Connection dead or missing, reconnecting...")
                    try:
                        self._initialize_connection()
                    except Exception as e:
                        logger.error("Failed to initialize connection: %s", str(e))
                        return {"error": f"Database connection error: {str(e)}"}

                try:
                    # Execute the query with proper parameter binding
                    result = self._conn.execute(query, bound_params).fetchdf()
                    
                    # Convert DataFrame to list of dictionaries
                    rows = result.to_dict('records')
                    
                    logger.debug("Query returned %d rows", len(rows))
                    return rows
                    
                except Exception as e:
                    msg = f"Query execution error: {str(e)}"
                    logger.error(msg)
                    return {"error": msg}
                        
        except Exception as e:
            msg = f"Unexpected error: {str(e)}"
            logger.error(msg)
            return {"error": msg}

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
