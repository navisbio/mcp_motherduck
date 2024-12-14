import logging
import os
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

import mcp.types as types
from .memo_manager import MemoManager

logger = logging.getLogger('mcp_bigquery_biomedical.tools')


class ToolManager:
    def __init__(self, memo_manager: MemoManager):
        self.memo_manager = memo_manager
        self.bigquery_client = self._initialize_bigquery_client()
        logger.info("ToolManager initialized with BigQuery client")

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
                description="Execute a SELECT query on the BigQuery Open Targets platform dataset. Use this tool to extract and analyze specific data from any table.",
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
                description="Get an overview of all available tables in the Open Targets platform dataset. This tool helps you understand the dataset structure before starting your analysis to identify relevant data sources.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="describe-table",
                description="Examine the detailed structure of a specific table in the Open Targets platform dataset, including column names and data types. Use this before querying to ensure you target the right columns and understand the data format.",
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
                description="Record key findings and insights discovered during your analysis. Use this tool whenever you uncover meaningful patterns, trends, or notable observations about the data. This helps build a comprehensive analytical narrative and ensures important discoveries are documented.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "finding": {"type": "string", "description": "Analysis finding about data patterns or trends"},
                    },
                    "required": ["finding"],
                },
            ),
            types.Tool(
                name="search-gene-names",
                description="Search for gene names in the Open Targets BigQuery dataset based on a search query.",
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

            if not arguments and name != "list-tables":
                logger.error("Missing required arguments for tool execution")
                raise ValueError("Missing required arguments")

            if name == "list-tables":
                logger.debug("Listing tables from BigQuery dataset")
                query = """
                    SELECT table_name
                    FROM `bigquery-public-data.open_targets_platform.INFORMATION_SCHEMA.TABLES`
                    ORDER BY table_name;
                """
                query_job = self.bigquery_client.query(query)
                results = query_job.result()
                tables = [row.table_name for row in results]
                logger.info(f"Retrieved {len(tables)} tables")
                return [types.TextContent(type="text", text=str(tables))]

            elif name == "describe-table":
                if "table_name" not in arguments:
                    logger.error("Missing table_name argument for describe-table")
                    raise ValueError("Missing table_name argument")

                table_name = arguments["table_name"]
                logger.debug(f"Describing table: {table_name}")
                query = f"""
                    SELECT column_name, data_type, is_nullable
                    FROM `bigquery-public-data.open_targets_platform.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = @table_name
                    ORDER BY ordinal_position;
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
                    ]
                )
                query_job = self.bigquery_client.query(query, job_config=job_config)
                results = query_job.result()
                columns = [{"column_name": row.column_name, "data_type": row.data_type, "is_nullable": row.is_nullable} for row in results]
                logger.info(f"Retrieved {len(columns)} columns for table {table_name}")
                return [types.TextContent(type="text", text=str(columns))]

            elif name == "read-query":
                query = arguments.get("query", "").strip()

                logger.debug(f"Executing query: {query}")
                query_job = self.bigquery_client.query(query)
                results = query_job.result()
                rows = [dict(row) for row in results]
                logger.info(f"Query returned {len(rows)} rows")
                return [types.TextContent(type="text", text=str(rows))]

            elif name == "append-insight":
                if "finding" not in arguments:
                    logger.error("Missing finding argument for append-insight")
                    raise ValueError("Missing finding argument")

                finding = arguments["finding"]
                logger.debug(f"Adding insight: {finding[:50]}...")
                self.memo_manager.add_insights(finding)
                logger.info("Insight added successfully")
                return [types.TextContent(type="text", text="Insight added")]

            elif name == "search-gene-names":
                if "search" not in arguments:
                    logger.error("Missing search argument for search-gene-names")
                    raise ValueError("Missing search argument")

                search_query = arguments["search"]
                logger.debug(f"Searching for gene names with query: {search_query}")
                gene_names = await self.get_gene_names_service(search_query)
                logger.info(f"Retrieved {len(gene_names)} gene names")
                return [types.TextContent(type="text", text=str(gene_names))]

        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
            raise

    async def get_gene_names_service(self, search: str) -> list[str]:
        """Searches for gene names in the Open Targets BigQuery dataset."""
        search_pattern = f"%{search.lower()}%"
        query = """
            SELECT DISTINCT approvedSymbol
            FROM `bigquery-public-data.open_targets_platform.targets`
            WHERE LOWER(approvedSymbol) LIKE @search
                OR LOWER(id) LIKE @search
                OR LOWER(approvedName) LIKE @search
                OR EXISTS(SELECT 1 FROM UNNEST(symbolSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search)
                OR EXISTS(SELECT 1 FROM UNNEST(nameSynonyms.list) syn WHERE LOWER(syn.element.label) LIKE @search)
                OR EXISTS(SELECT 1 FROM UNNEST(obsoleteNames.list) syn WHERE LOWER(syn.element.label) LIKE @search)
            LIMIT 50;
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("search", "STRING", search_pattern)
            ]
        )
        logger.debug("Running BigQuery job for gene name search")
        query_job = self.bigquery_client.query(query, job_config=job_config)
        results = query_job.result()
        gene_names = [row.approvedSymbol for row in results]
        logger.debug(f"Gene names retrieved: {gene_names}")
        return gene_names