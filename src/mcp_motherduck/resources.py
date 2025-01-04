from typing import TypedDict
from pydantic import AnyUrl
import mcp.types as types

class ResourceDefinition(TypedDict):
    uri: str
    name: str
    description: str
    mimeType: str

MOTHERDUCK_RESOURCES: list[ResourceDefinition] = [
    {
        "uri": "schema://database",
        "name": "MotherDuck Database Schema",
        "description": "Detailed structural information about the MotherDuck database, including table relationships, column definitions, and data types. Essential reference for understanding data organization and planning effective queries.",
        "mimeType": "application/json",
    }
]

def get_resources() -> list[types.Resource]:
    return [
        types.Resource(
            uri=AnyUrl(resource["uri"]),
            name=resource["name"],
            description=resource["description"],
            mimeType=resource["mimeType"],
        )
        for resource in MOTHERDUCK_RESOURCES
    ] 