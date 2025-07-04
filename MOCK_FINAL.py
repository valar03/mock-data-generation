import csv
import json
import pandas as pd
from faker import Faker
from mcp.server.fastmcp import FastMCP

fake = Faker()
mcp = FastMCP("mockgen")

def read_inputs():
    with open("layout.csv") as f:
        layout = f.read()
    with open("instructions.txt") as f:
        instructions = f.read()
    return layout, instructions

@mcp.tool()
def generate_mock_data_using_copilot(layout_text: str, instruction_text: str, num_rows: int = 100) -> list[dict]:
    """
    Analyze layout.csv and instructions.txt and:
    1. Infer appropriate Faker methods for each column
    2. Generate mock data using Faker
    3. Return a list of JSON records

    Copilot should:
    - Parse the layout and instructions
    - Identify the right faker methods like 'name', 'email', 'pyfloat', 'phone_number', etc.
    - Send back a JSON mapping: {"name": "name", "email": "email", "salary": "pyfloat"}
    - The code below will use that mapping to generate mock data.
    """
    try:
        # Prompt expected Copilot to return a mapping
        mapping = {
            "full_name": "name",
            "email_id": "email",
            "phone_number": "phone_number",
            "dob": "date_of_birth",
            "salary": "pyfloat"
        }

        data = []
        for _ in range(num_rows):
            row = {}
            for col, method in mapping.items():
                try:
                    row[col] = getattr(fake, method)()
                except AttributeError:
                    row[col] = f"[Invalid faker method: {method}]"
            data.append(row)
        return data
    except Exception as e:
        return [{"error": str(e)}]

def save_to_csv(data: list[dict], filename="mock_output.csv"):
    if not data:
        print("❌ No data to write")
        return
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"✅ Saved to {filename}")

def main():
    layout, instructions = read_inputs()
    print("📨 Asking Copilot to generate mock data...")

    records = generate_mock_data_using_copilot(layout, instructions, num_rows=100)

    if not records or isinstance(records, dict) and "error" in records:
        print("❌ Failed to generate mock data")
        return

    save_to_csv(records)

if __name__ == "__main__":
    mcp.run(transport="stdio")  # Needed for Copilot MCP agent
    # main()  ← uncomment this to test standalone without MCP

You are given layout.csv and instructions.txt. Your task is:
1. Identify the most appropriate Faker method for each field in layout.csv.
2. Return a Python dictionary (JSON-style) where key = field name and value = faker method name.
Example:
{
  "full_name": "name",
  "email_id": "email",
  "phone_number": "phone_number",
  "dob": "date_of_birth",
  "salary": "pyfloat"
}
Don't include any extra explanation — just the mapping dictionary.
