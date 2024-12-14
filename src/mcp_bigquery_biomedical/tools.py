import logging
import os
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

import mcp.types as types
from .database import AACTDatabase
from .memo_manager import MemoManager

logger = logging.getLogger('mcp_aact_server.tools')


class ToolManager:
    def __init__(self, db: AACTDatabase, memo_manager: MemoManager):
        self.db = db
        self.memo_manager = memo_manager
        self.bigquery_client = self._initialize_bigquery_client()
        logger.info("ToolManager initialized")

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

    def get_available_tools(self) -> list[types.Tool]:
        """Return list of available tools."""
        logger.debug("Retrieving available tools")
        tools = [
            types.Tool(
                name="read-query",
                description="Execute a SELECT query on the AACT clinical trials database. Use this tool to extract and analyze specific data from any table.",
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
                description="Get an overview of all available tables in the AACT database. This tool helps you understand the database structure before starting your analysis to identify relevant data sources.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="describe-table",
                description="Examine the detailed structure of a specific AACT table, including column names and data types. Use this before querying to ensure you target the right columns and understand the data format.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Name of the table to describe"},
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="append-insight",
                description="Record key findings and insights discovered during your analysis. Use this tool whenever you uncover meaningful patterns, trends, or notable observations about clinical trials. This helps build a comprehensive analytical narrative and ensures important discoveries are documented.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "Analysis finding about trial patterns or trends"},
                    },
                    "required": ["finding"],
                },
            ),
            types.Tool(
                name="search-gene-names",
                description="Search for gene names in the OpenTargets BigQuery dataset based on a search query.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "search": {"type": "string", "description": "Search query for gene names"},
                    },
                    "required": ["search"],
                },
            ),
        ]
        logger.debug(f"Retrieved {len(tools)} available tools")
        return tools

    async def execute_tool(self, name: str, arguments: dict[str, Any] | None) -> list[types.TextContent]:
        """Execute a tool with given arguments."""
        logger.info(f"Executing tool: {name} with arguments: {arguments}")

        try:
            available_tool_names = {tool.name for tool in self.get_available_tools()}
            if name not in available_tool_names:
                logger.error(f"Unknown tool requested: {name}")
                raise ValueError(f"Unknown tool: {name}")

            if not arguments and name not in {"list-tables"}:
                logger.error("Missing required arguments for tool execution")
                raise ValueError("Missing required arguments")

            if name == "list-tables":
                logger.debug("Executing list-tables query")
                results = self.db.execute_query("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'ctgov'
                    ORDER BY table_name;
                """)
                logger.info(f"Retrieved {len(results)} tables")
                return [types.TextContent(type="text", text=str(results))]

            elif name == "describe-table":
                if "table_name" not in arguments:
                    logger.error("Missing table_name argument for describe-table")
                    raise ValueError("Missing table_name argument")

                logger.debug(f"Describing table: {arguments['table_name']}")
                results = self.db.execute_query("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = 'ctgov' 
                    AND table_name = %s
                    ORDER BY ordinal_position;
                """, {"table_name": arguments["table_name"]})
                logger.info(f"Retrieved {len(results)} columns for table {arguments['table_name']}")
                return [types.TextContent(type="text", text=str(results))]

            elif name == "read-query":
                query = arguments.get("query", "").strip()

                logger.debug(f"Executing query: {query}")
                results = self.db.execute_query(query)
                logger.info(f"Query returned {len(results)} rows")
                return [types.TextContent(type="text", text=str(results))]

            elif name == "append-insight":
                if "finding" not in arguments:
                    logger.error("Missing finding argument for append-insight")
                    raise ValueError("Missing finding argument")

                logger.debug(f"Adding insight: {arguments['finding'][:50]}...")
                self.memo_manager.add_insights(arguments["finding"])
                logger.info("Insight added successfully")
                return [types.TextContent(type="text", text="Insight added")]

            elif name == "search-gene-names":
                if "search" not in arguments:
                    logger.error("Missing search argument for search-gene-names")
                    raise ValueError("Missing search argument")

                search_query = arguments["search"]
                logger.debug(f"Searching for gene names with query: {search_query}")
                # Await the async function call
                gene_names = await self.get_gene_names_service(search_query)
                logger.info(f"Retrieved {len(gene_names)} gene names")
                return [types.TextContent(type="text", text=str(gene_names))]


        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise

    async def get_gene_names_service(self, search: str) -> list[str]:
        """Searches for gene names in the OpenTargets BigQuery dataset."""
        query = """
        SELECT DISTINCT approvedSymbol, sort_order
        FROM (
            SELECT
                approvedSymbol,
                CASE
                    WHEN LOWER(approvedSymbol) LIKE @search THEN 1
                    WHEN LOWER(id) LIKE @search THEN 2
                    WHEN LOWER(approvedName) LIKE @search THEN 3
                    WHEN EXISTS(SELECT 1 FROM UNNEST(symbolSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search) THEN 4
                    WHEN EXISTS(SELECT 1 FROM UNNEST(nameSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search) THEN 5
                    WHEN EXISTS(SELECT 1 FROM UNNEST(obsoleteNames.list) syn WHERE LOWER(syn.element.label) LIKE @search) THEN 6
                    ELSE 7
                END AS sort_order
            FROM `bigquery-public-data.open_targets_platform.targets`
            WHERE biotype = 'protein_coding'
            AND (
                LOWER(id) LIKE @search
                OR LOWER(approvedSymbol) LIKE @search
                OR LOWER(approvedName) LIKE @search
                OR EXISTS(SELECT 1 FROM UNNEST(symbolSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search)
                OR EXISTS(SELECT 1 FROM UNNEST(nameSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search)
                OR EXISTS(SELECT 1 FROM UNNEST(obsoleteNames.list) syn WHERE LOWER(syn.element.label) LIKE @search)
            )
        )
        ORDER BY
            sort_order,
            approvedSymbol
        LIMIT 50
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("search", "STRING", f"%{search.lower()}%")
            ]
        )
        logger.debug("Running BigQuery job for gene name search")
        query_job = self.bigquery_client.query(query, job_config=job_config)
        results = query_job.result()
        gene_names = [row.approvedSymbol for row in results]
        logger.debug(f"Gene names retrieved: {gene_names}")
        return gene_names
