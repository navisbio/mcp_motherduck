from . import server
import asyncio
import argparse
import logging

def main():
    """Main entry point for the MotherDuck MCP server."""
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='MotherDuck MCP Server')
    args = parser.parse_args()
    asyncio.run(server.main())

__all__ = ["main", "server"]
