from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mockgen")
API_BASE = "http://localhost:5000"

# ------------ Flask API Calls ------------

async def post(endpoint: str, data: dict = {}) -> dict[str, Any]:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{API_BASE}{endpoint}", json=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

# ------------ MCP Tools ------------

@mcp.tool()
async def upload_files() -> str:
    """Upload layout.csv and instructions.txt to backend memory."""
    try:
        with open("layout.csv") as lf, open("instructions.txt") as inf:
            layout = lf.read()
            instructions = inf.read()
        response = await post("/upload", {"layout": layout, "instructions": instructions})
        return response["message"]
    except Exception as e:
        return f"❌ Error uploading files: {str(e)}"

@mcp.tool()
async def generate_data(records: int = 500) -> str:
    """Generate mock data using the stored mapping."""
    response = await post("/generate", {"records": records})
    return response.get("message", "❌ Generation failed")

@mcp.tool()
async def export_csv(filename: str = "mock_output.csv") -> str:
    """Export the generated data to CSV."""
    response = await post("/export", {"filename": filename})
    return response.get("message", "❌ Export failed")

# ------------ MCP Expect Hook ------------

@mcp.expect(schema={"type": "object", "patternProperties": {".*": {"type": "string"}}})
async def auto_store_mapping(data: dict[str, Any]):
    """Automatically store mapping when a valid JSON mapping is returned by Copilot."""
    await post("/store_mapping", {"mapping": data})
    print("✅ Detected and stored Faker mapping automatically.")

# ------------ Run Server ------------

if __name__ == "__main__":
    mcp.run(transport="stdio")



######

from flask import Flask, request, jsonify
from faker import Faker
import pandas as pd

app = Flask(__name__)
faker = Faker()

# -------- In-memory Store --------
memory = {
    "layout": "",
    "instructions": "",
    "mapping": {},
    "mock_data": []
}

@app.route("/upload", methods=["POST"])
def upload():
    memory["layout"] = request.json.get("layout", "")
    memory["instructions"] = request.json.get("instructions", "")
    return jsonify({"message": "✅ Layout and instructions uploaded."})

@app.route("/store_mapping", methods=["POST"])
def store_mapping():
    mapping = request.json.get("mapping", {})
    if not isinstance(mapping, dict):
        return jsonify({"message": "❌ Invalid mapping format."}), 400
    memory["mapping"] = mapping
    return jsonify({"message": f"✅ Mapping stored: {mapping}"})

@app.route("/generate", methods=["POST"])
def generate():
    mapping = memory.get("mapping", {})
    record_count = request.json.get("records", 500)
    if not mapping:
        return jsonify({"message": "❌ No mapping available to generate data."})
    
    try:
        data = []
        for _ in range(record_count):
            row = {}
            for col, faker_fn in mapping.items():
                if hasattr(faker, faker_fn):
                    row[col] = getattr(faker, faker_fn)()
                else:
                    row[col] = f"<missing:{faker_fn}>"
            data.append(row)
        memory["mock_data"] = data
        return jsonify({"message": f"✅ Generated {record_count} records."})
    except Exception as e:
        return jsonify({"message": f"❌ Error generating data: {str(e)}"})

@app.route("/export", methods=["POST"])
def export():
    filename = request.json.get("filename", "mock_output.csv")
    try:
        df = pd.DataFrame(memory["mock_data"])
        df.to_csv(filename, index=False)
        return jsonify({"message": f"✅ Mock data exported to {filename}."})
    except Exception as e:
        return jsonify({"message": f"❌ Export error: {str(e)}"})

if __name__ == "__main__":
    app.run(port=5000)


$#######

