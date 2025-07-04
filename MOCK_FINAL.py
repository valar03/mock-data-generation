import csv
import json
from typing import Any
from faker import Faker
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("mockdata-gen")

# Global variables
layout_text = ""
instructions_text = ""
faker_field_map = {}
mock_data = []

faker = Faker()

# ---------------- MCP Tools ----------------

@mcp.tool()
def load_layout_and_instructions() -> str:
    """
    Reads layout.csv and instructions.txt and stores them in global memory.
    """
    global layout_text, instructions_text
    with open("layout.csv", "r") as f:
        layout_text = f.read()
    with open("instructions.txt", "r") as f:
        instructions_text = f.read()
    return "✅ Loaded layout and instructions."

@mcp.tool()
def identify_faker_fields() -> str:
    """
    Copilot will infer appropriate faker fields for each column.
    Prompt Copilot with: 'identify faker fields'
    """
    global layout_text, instructions_text
    return f"""🎯 Prompt:
Given the layout of a dataset below and a description of data generation rules, return a Python dictionary where:
- Each key is a column name
- Each value is the most suitable Faker method name (like 'name', 'email', 'pyint', 'pyfloat', 'date_of_birth', etc.)
Return only a valid JSON dictionary.

Here is the layout:
{layout_text}

Here are the rules:
{instructions_text}
"""

@mcp.tool()
def store_faker_mapping(mapping_json: str) -> str:
    """
    Stores the faker field mapping returned by Copilot into global memory.
    """
    global faker_field_map
    try:
        faker_field_map = json.loads(mapping_json)
        return f"✅ Stored faker field mapping for {len(faker_field_map)} columns."
    except Exception as e:
        return f"❌ Error parsing mapping JSON: {e}"

@mcp.tool()
def generate_mock_data(count: int = 10) -> str:
    """
    Generates mock data using the stored faker_field_map.
    """
    global mock_data
    if not faker_field_map:
        return "❌ No faker field mapping found. Run `identify_faker_fields` first."

    mock_data = []
    for _ in range(count):
        row = {}
        for col, method in faker_field_map.items():
            try:
                row[col] = getattr(faker, method)() if hasattr(faker, method) else f"<{method}>"
            except Exception:
                row[col] = f"<invalid:{method}>"
        mock_data.append(row)
    return f"✅ Generated {count} rows of mock data."

@mcp.tool()
def save_mock_data_to_csv(filename: str = "mock_output.csv") -> str:
    """
    Saves generated mock data to a CSV file.
    """
    if not mock_data:
        return "❌ No mock data to save. Run `generate_mock_data` first."

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=mock_data[0].keys())
        writer.writeheader()
        writer.writerows(mock_data)
    return f"✅ Mock data saved to {filename}."

# ---------------- Run Server ----------------
if __name__ == "__main__":
    mcp.run(transport="stdio")
