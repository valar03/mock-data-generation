from mcp_devx.fastmcp import FastMCP
from mcp_devx.llm import complete
import os
import re
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

mcp = FastMCP("mock-data-generator")

def remove_non_ascii(text: str) -> str:
    return re.sub(r'[^\x00-\x7F]+', '', text)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
layout_path = os.path.join(BASE_DIR, "input", "layout.csv")
instruction_path = os.path.join(BASE_DIR, "input", "instruction.txt")
output_path = os.path.join(BASE_DIR, "generated_data.csv")

@mcp.tool()
async def generate_mock_data() -> str:
    """Generate mock CSV data using Copilot DevX LLM response."""

    try:
        with open(layout_path, "r", encoding="utf-8") as f:
            layout = f.read()
        with open(instruction_path, "r", encoding="utf-8") as f:
            instructions = f.read()
    except Exception as e:
        return f"❌ Failed to read input files: {e}"

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

    try:
        # ✅ Real Copilot response via DevX LLM
        response = await complete(prompt=prompt)

        # Clean and encode result
        cleaned = remove_non_ascii(response)
        cleaned = cleaned.encode("ascii", errors="ignore").decode("ascii")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned)

        return f"✅ Mock data written to {output_path}"
    except Exception as e:
        return f"❌ Error during Copilot generation: {e}"
