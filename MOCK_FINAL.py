import csv
import json
from typing import Any
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mockgen")

# Read layout.csv and instructions.txt
def read_input_files():
    with open("layout.csv", "r") as f:
        reader = csv.DictReader(f)
        layout = [row for row in reader]

    with open("instructions.txt", "r") as f:
        instructions = f.read()

    return layout, instructions

# Format prompt for GitHub Copilot
def build_prompt(layout, instructions):
    prompt = "You are a mock data generator.\n"
    prompt += "You are given the following layout for data generation:\n\n"
    for row in layout:
        prompt += f"- Column: {row['column_name']}, Type: {row['data_type']}"
        if row.get("constraints"):
            prompt += f", Constraints: {row['constraints']}"
        prompt += "\n"

    prompt += "\nInstructions:\n" + instructions
    prompt += "\n\nNow generate 10 rows of data in JSON format (list of dicts)."
    return prompt

@mcp.tool()
async def generate_mock_data() -> str:
    layout, instructions = read_input_files()
    prompt = build_prompt(layout, instructions)

    # Call GitHub Copilot through DevX
    response = await mcp.copilot(prompt)

    try:
        parsed = json.loads(response)
        assert isinstance(parsed, list)
        # Write to CSV
        if parsed:
            keys = parsed[0].keys()
            with open("mock_output.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(parsed)
        return "✅ Mock data generated and saved to mock_output.csv"
    except Exception as e:
        return f"❌ Failed to parse or write data: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="stdio")
