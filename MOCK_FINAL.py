from mcp.server.fastmcp import FastMCP
import httpx

mcp = FastMCP("mockgen")

API_BASE = "http://localhost:5000"

# 🟢 Upload files tool
@mcp.tool()
async def upload_layout_and_instructions() -> str:
    """Reads layout.csv and instructions.txt and uploads to Flask server."""
    try:
        with open("layout.csv") as f1, open("instructions.txt") as f2:
            layout = f1.read()
            instructions = f2.read()

        async with httpx.AsyncClient() as client:
            res = await client.post(f"{API_BASE}/upload", json={
                "layout": layout,
                "instructions": instructions
            })
            res.raise_for_status()
            return "✅ Uploaded layout and instructions successfully."
    except Exception as e:
        return f"❌ Upload failed: {e}"

# 🟢 Capture Faker mapping directly from Copilot
@mcp.tool(return_value=True)
async def capture_faker_mapping() -> str:
    """
    Copilot should return a JSON string mapping column names to Faker fields.
    This tool stores that mapping in memory automatically.
    """
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{API_BASE}/capture-mapping", json={
            "response": mcp.last_value
        })
        res.raise_for_status()
        return "✅ Mapping captured."

# 🟢 Generate and store mock data
@mcp.tool()
async def generate_mock_data() -> str:
    """Generate mock data using stored layout, instructions, and mapping."""
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(f"{API_BASE}/generate")
            res.raise_for_status()
            return "✅ Mock data generated and saved to 'mock_output.csv'."
    except Exception as e:
        return f"❌ Generation failed: {e}"

if __name__ == "__main__":
    mcp.run(transport="stdio")


##########################

from flask import Flask, request, jsonify
from faker import Faker
import pandas as pd
import json

app = Flask(__name__)
faker = Faker()

# 🔁 In-memory store
memory = {
    "layout": None,
    "instructions": None,
    "mapping": None,
}

@app.route("/upload", methods=["POST"])
def upload():
    data = request.get_json()
    memory["layout"] = data.get("layout")
    memory["instructions"] = data.get("instructions")
    return jsonify({"message": "Layout and instructions stored."})

@app.route("/capture-mapping", methods=["POST"])
def capture_mapping():
    data = request.get_json()
    try:
        # Ensure valid JSON string
        mapping = json.loads(data.get("response"))
        memory["mapping"] = mapping
        return jsonify({"message": "Mapping stored."})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/generate", methods=["POST"])
def generate():
    layout = memory.get("layout")
    mapping = memory.get("mapping")
    if not layout or not mapping:
        return jsonify({"error": "Missing layout or mapping."}), 400

    try:
        # Extract column names from layout
        lines = layout.strip().split("\n")
        headers = [line.split(",")[0].strip() for line in lines[1:]]

        data = []
        for _ in range(500):  # number of mock rows
            row = {}
            for col in headers:
                method = mapping.get(col)
                if method and hasattr(faker, method):
                    value = getattr(faker, method)()
                    row[col] = value
                else:
                    row[col] = ""
            data.append(row)

        df = pd.DataFrame(data)
        df.to_csv("mock_output.csv", index=False)
        return jsonify({"message": "Mock data saved."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5000)
