from mcp.server.fastmcp import FastMCP
from typing import List
import csv
import json
import os

mcp = FastMCP("mock-data-generator")

@mcp.tool()
async def generate_mock_data_from_layout(layout_path: str, instruction_path: str, output_path: str = "generated_data.csv", num_rows: int = 5) -> str:
    """Generate mock data from layout and instructions using LLM via Copilot."""
    try:
        # Read layout
        with open(layout_path, "r") as f:
            layout = f.read()

        # Read instructions
        with open(instruction_path, "r") as f:
            instructions = f.read()

        # 👇 Construct prompt for Copilot
        prompt = f"""
Generate {num_rows} rows of mock data as CSV using this layout and rules.

Layout:
{layout}

Instructions:
{instructions}

Only return CSV data with headers.
        """.strip()

        # 🧠 Ask Copilot to continue from here (DevX will pick up)
        print("\n👇 Paste this into Copilot Chat or allow DevX to auto-call tool:\n")
        print(prompt)

        # 👉 Simulate LLM response (in real DevX, LLM will fill this)
        mock_response = """
name,age,senior_status
Alice,70,Yes
Bob,45,No
Carol,67,Yes
David,34,No
Eve,80,Yes
""".strip()

        # Write to output CSV
        with open(output_path, "w") as out_f:
            out_f.write(mock_response)

        return f"Mock data written to {output_path}"

    except Exception as e:
        return f"Error generating mock data: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="stdio")


name,age,senior_status
string,int,string


- age should be between 25 and 90
- if age > 60, senior_status should be "Yes", else "No"
- names can be common English names
