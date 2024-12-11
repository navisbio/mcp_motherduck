from . import server
import asyncio
import argparse

def main():
    """Main entry point for the MotherDuck MCP server."""
    parser = argparse.ArgumentParser(description='MotherDuck MCP Server')
    args = parser.parse_args()
    asyncio.run(server.main())

__all__ = ["main", "server"]
