import logging
import os
from typing import Any, Optional
from google.cloud import bigquery
from google.oauth2 import service_account


from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger('mcp_aact_server.database')

class BigQueryDatabase:
    def __init__(self):
        logger.info("Initializing BigQuery database connection")
        self.client = self._initialize_bigquery_client()
        logger.info("BigQuery database initialization complete")

    def _initialize_bigquery_client(self) -> bigquery.Client:
        """Initializes the BigQuery client."""
        logger.debug("Initializing BigQuery client")
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info("BigQuery client initialized")
        return client

    def execute_query(self, query: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries"""
        logger.debug(f"Executing query: {query}")
        job_config = None
        if params:
            logger.debug(f"Query parameters: {params}")
            query_parameters = [
                bigquery.ScalarQueryParameter(name, "STRING", value)
                for name, value in params.items()
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            rows = [dict(row) for row in results]
            logger.debug(f"Query returned {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"BigQuery error executing query: {str(e)}", exc_info=True)
            raise RuntimeError(f"BigQuery error: {str(e)}")
