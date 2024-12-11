import pytest
import json
from dotenv import load_dotenv
import os
from unittest.mock import Mock
from mcp.types import Tool, TextContent
from mcp_server_motherduck.tools import ToolManager
from mcp_server_motherduck.database import MotherDuckDatabase
from mcp_server_motherduck.memo_manager import MemoManager

@pytest.fixture
def mock_db():
    db = Mock(spec=MotherDuckDatabase)
    db.database = 'test_db'  # Set a test database name to prevent AttributeError
    return db

@pytest.fixture
def mock_memo_manager():
    return Mock(spec=MemoManager)

@pytest.fixture
def tool_manager(mock_db, mock_memo_manager):
    return ToolManager(mock_db, mock_memo_manager)

@pytest.fixture(scope="session", autouse=True)
def setup_motherduck_env():
    """Setup MotherDuck environment for integration tests"""
    load_dotenv()  # This will load environment variables from .env file
    if "MOTHERDUCK_TOKEN" not in os.environ:
        pytest.skip("MOTHERDUCK_TOKEN environment variable not set")

# Test tool availability
def test_get_available_tools(tool_manager):
    """
    Verify that all expected tools are registered and available
    This ensures our tool interface remains stable
    """
    tools = tool_manager.get_available_tools()
    assert len(tools) == 4
    tool_names = {tool.name for tool in tools}
    expected_tools = {"read-query", "list-tables", "describe-table", "append-analysis"}
    assert tool_names == expected_tools

# Test invalid tool name
@pytest.mark.asyncio
async def test_execute_tool_invalid_name(tool_manager):
    """
    Verify that invalid tool names are properly rejected
    This prevents calling non-existent tools
    """
    with pytest.raises(ValueError, match="Unknown tool"):
        await tool_manager.execute_tool("non-existent-tool", {})

# Test read-query blocks unsafe operations
@pytest.mark.asyncio
async def test_read_query_blocks_unsafe_operations(tool_manager):
    """
    Verify that unsafe SQL operations are blocked
    This prevents potential security issues
    """
    unsafe_queries = [
        "SELECT * FROM users; DROP TABLE users;",
        "SELECT * FROM table; INSERT INTO table VALUES (1);",
        "PRAGMA table_info('users');"
    ]
    for query in unsafe_queries:
        with pytest.raises(ValueError, match="Only SELECT statements are allowed."):
            await tool_manager.execute_tool("read-query", {"query": query})

# Test real query execution
@pytest.mark.asyncio
async def test_real_query_execution(tool_manager, mock_db):
    """
    Test executing a real query using the read-query tool
    """
    query = "SELECT id, name FROM test_table"

    # Mock the results returned by the database
    mock_results = [
        (1, 'Alice'),
        (2, 'Bob'),
    ]
    mock_columns = ['id', 'name']
    mock_db.execute_query.return_value = (mock_results, mock_columns)

    arguments = {"query": query}
    response = await tool_manager.execute_tool("read-query", arguments)
    expected_output = [
        {
            "id": 1,
            "name": "Alice"
        },
        {
            "id": 2,
            "name": "Bob"
        }
    ]
    assert len(response) == 1
    assert isinstance(response[0], TextContent)
    assert json.loads(response[0].text) == expected_output

# Test listing tables
@pytest.mark.asyncio
async def test_real_table_listing(tool_manager, mock_db):
    """
    Test listing tables using the list-tables tool
    """
    # Mock the results returned by the database
    mock_results = [
        ('another_table',),
        ('test_table',),
    ]
    mock_columns = ['name']
    mock_db.execute_query.return_value = (mock_results, mock_columns)

    response = await tool_manager.execute_tool("list-tables", {})
    expected_output = "another_table\ntest_table"
    assert len(response) == 1
    assert isinstance(response[0], TextContent)
    assert response[0].text == expected_output

# Test describe-table tool
@pytest.mark.asyncio
async def test_describe_table(tool_manager, mock_db):
    """
    Test describing a table using the describe-table tool
    """
    table_name = "test_table"
    # Mock the results returned by the database
    mock_results = [
        (0, 'id', 'INTEGER', 0, None, 0),
        (1, 'name', 'TEXT', 0, None, 0),
    ]
    mock_columns = ['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk']
    mock_db.execute_query.return_value = (mock_results, mock_columns)

    arguments = {"table_name": table_name}
    response = await tool_manager.execute_tool("describe-table", arguments)
    expected_output = "id | INTEGER | NULLABLE\nname | TEXT | NULLABLE"
    assert len(response) == 1
    assert isinstance(response[0], TextContent)
    assert response[0].text == expected_output

    # Test when table does not exist
    mock_db.execute_query.return_value = ([], mock_columns)
    response = await tool_manager.execute_tool("describe-table", arguments)
    expected_output = "Table 'test_table' does not exist."
    assert response[0].text == expected_output

# Test append-analysis tool
@pytest.mark.asyncio
async def test_append_analysis(tool_manager, mock_memo_manager):
    """
    Test adding a finding using the append-analysis tool
    """
    finding = "This is a test finding."
    arguments = {"finding": finding}
    response = await tool_manager.execute_tool("append-analysis", arguments)
    mock_memo_manager.add_finding.assert_called_with(finding)
    assert len(response) == 1
    assert isinstance(response[0], TextContent)
    assert response[0].text == "Analysis finding added"
