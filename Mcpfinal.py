from mcp.server.fastmcp import FastMCP
import os
import re
import sys
import io

# Force stdout to use UTF-8 to avoid encoding issues (Windows-safe)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

mcp = FastMCP("mock-data-generator")

# Sanitize Copilot/GPT response to remove emojis or non-ASCII characters
def remove_non_ascii(text: str) -> str:
    return re.sub(r'[^\x00-\x7F]+', '', text)

# Hardcoded absolute or relative-safe file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
layout_path = os.path.join(BASE_DIR, "input", "layout.csv")
instruction_path = os.path.join(BASE_DIR, "input", "instruction.txt")
output_path = os.path.join(BASE_DIR, "generated_data.csv")

@mcp.tool()
async def generate_mock_data() -> str:
    """
    Generate mock tabular data based on layout.csv and instruction.txt.
    Uses a prompt to ask Copilot or GPT-like LLM for synthetic CSV data.
    """

    # Step 1: Read input files
    try:
        with open(layout_path, "r", encoding="utf-8") as f:
            layout = f.read()

        with open(instruction_path, "r", encoding="utf-8") as f:
            instructions = f.read()
    except Exception as e:
        return f"❌ Failed to read input files: {e}"

    # Step 2: Construct prompt for GPT (simulated)
    prompt = f"""
Generate 5 rows of mock data in CSV format using this layout and these instructions.

Layout:
{layout}

Instructions:
{instructions}

‼️ IMPORTANT: Do NOT include any emojis (e.g., 👈, ✅, ❌) or special Unicode characters.
Only use plain ASCII characters.

Respond ONLY with valid CSV content with headers.
    """.strip()

    # Step 3: Simulated Copilot/GPT response (replace with actual call)
    simulated_response = """
name,age,senior_status
Alice,70,Yes
Bob,45,No
Carol,67,Yes
David,34,No
Eve,80,Yes
""".strip()

    # Step 4: Clean response
    clean_response = remove_non_ascii(simulated_response)
    clean_response = clean_response.encode("ascii", errors="ignore").decode("ascii")

    # Step 5: Write to output CSV
    try:
        with open(output_path, "w", encoding="utf-8") as out_f:
            out_f.write(clean_response)
    except Exception as e:
        return f"❌ Failed to write output file: {e}"

    return f"✅ Mock data written to {output_path}"

# Run the MCP server
if __name__ == "__main__":
    # You can test manually without Copilot like this:
    import asyncio
    result = asyncio.run(generate_mock_data())
    print(result)

    # Uncomment below to expose to DevX
    # mcp.run(transport="stdio")
