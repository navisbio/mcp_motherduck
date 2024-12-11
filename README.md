# MotherDuck MCP Server

## Overview
A Model Context Protocol (MCP) server implementation that provides access to MotherDuck databases. This server enables data analysis and automatically generates analysis memos that capture insights from your data.

## Components

### Resources
The server exposes a dynamic resource to store analysis results:
- `memo://analysis`: Key findings and insights from data analysis

### Tools
The server offers several core tools:

#### Query Tools
- `read-query`
   - Execute SELECT queries on the MotherDuck database
   - Input: 
     - `query` (string): The SELECT SQL query to execute
   - Returns: Query results as JSON array of objects

#### Schema Tools
- `list-tables`
   - Get a list of all tables in the MotherDuck database
   - No input required
   - Returns: List of table names

- `describe-table`
   - View schema information for a specific table
   - Input:
     - `table_name` (string): Name of table to describe
   - Returns: Column definitions with names, types, and nullability

#### Analysis Tools
- `append-analysis`
   - Add new findings to the analysis memo
   - Input:
     - `finding` (string): Analysis finding about patterns or trends
   - Returns: Confirmation of finding addition

## Environment Variables
The server requires the following environment variables:
- `MOTHERDUCK_TOKEN`: Your MotherDuck authentication token
- `MOTHERDUCK_DATABASE`: Name of the MotherDuck database to connect to

## Usage with Claude Desktop

Add the following to your claude_desktop_config.json:


"mcpServers": {
    "MOTHERDUCK-MCP": {
      "command": "python",
      "args": [
        "-m",
        "mcp_server_motherduck"
      ],
      "env": {
        "MOTHERDUCK_TOKEN": "YOUR_TOKEN",
        "MOTHERDUCK_DATABASE": "YOUR_DATABASE"
      }
    }
}


## Roadmap & Contribution

Over the coming weeks and months, we will build other MCP servers for the following datasets:

- OpenFDA: Access to FDA drug, device, and food data
- ChEMBL: Bioactive molecules and drug-like compounds
- Open Targets: Genetic associations and drug target validation
- And more to come!

We warmly welcome contributions of all kinds! Happy to hear from you if you

- Have specific use cases you'd like to explore
- Need customizations for your research
- Want to suggest additional datasets
- Are interested in contributing code, documentation, or ideas
- Want to improve existing features

Please reach out by:
- Opening an issue on GitHub
- Starting a discussion in our repository
- Emailing us at jonas.walheim@navis-bio.com
- Submitting pull requests

Your feedback helps shape our development priorities and align them with the research community's needs.

## License

This MCP server is licensed under the GNU General Public License v3.0 (GPL-3.0). This means you have the freedom to run, study, share, and modify the software. Any modifications or derivative works must also be distributed under the same GPL-3.0 terms. For more details, please see the LICENSE file in the project repository.
