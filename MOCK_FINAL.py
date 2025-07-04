import csv
import pandas as pd
from typing import Any
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mock-data")

@mcp.tool()
def generate_mock_data(layout_file: str = "layout.csv", instruction_file: str = "instructions.txt") -> list[dict[str, Any]]:
    """
    Generate mock data based on layout and instructions.

    Arguments:
        layout_file: Path to layout CSV (e.g., column_name, data_type, constraints)
        instruction_file: Path to natural language instructions

    Returns:
        A list of dictionaries representing mock records (ready to save as CSV)
    """
    layout = []
    with open(layout_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            layout.append(row)

    with open(instruction_file, "r") as f:
        instructions = f.read()

    # 👇 Copilot will fill logic here using layout + instructions
    return []  # Copilot will generate a list of records like: [{"name": "Alice", "age": 30}, ...]


Please populate the generate_mock_data() tool. It should:
1. Read the layout.csv for column names, types, and constraints.
2. Read instructions.txt for additional rules.
3. Use the Faker library or hardcoded logic to generate 50 mock records accordingly.

Return a list of dictionaries like:
[{"name": "Alice", "age": 32, "email": "alice@example.com"}, ...]

Don't leave return [] empty — return actual mock data using parsed schema.

@mcp.tool()
def save_to_csv(data: list[dict[str, Any]], output_file: str = "mock_output.csv") -> str:
    """
    Save mock data to a CSV file.

    Arguments:
        data: List of mock records (from generate_mock_data)
        output_file: Output CSV file name

    Returns:
        Confirmation message on success
    """
    if not data:
        return "No data to save."
    
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    return f"✅ Mock data saved to {output_file}"


if __name__ == "__main__":
    mcp.run(transport="stdio")

Please populate the generate_mock_data() tool. It should:
1. Read the layout.csv for column names, types, and constraints.
2. Read instructions.txt for additional rules.
3. Use the Faker library or hardcoded logic to generate 50 mock records accordingly.

Return a list of dictionaries like:
[{"name": "Alice", "age": 32, "email": "alice@example.com"}, ...]

Don't leave return [] empty — return actual mock data using parsed schema.

Please populate the generate_mock_data() function. It should read layout.csv for schema and generate 50 mock records using Faker. Return a list of dicts.
