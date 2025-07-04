# app.py
from flask import Flask, request, jsonify
import pandas as pd
from faker import Faker
import json

app = Flask(__name__)

faker = Faker()

# Global storage
layout_content = ""
instructions_content = ""
faker_mapping = {}
mock_data = []

@app.route("/upload", methods=["POST"])
def upload_files():
    global layout_content, instructions_content
    layout_content = request.form.get("layout", "")
    instructions_content = request.form.get("instructions", "")
    return jsonify({"message": "Files uploaded successfully"}), 200

@app.route("/prompt_payload", methods=["GET"])
def get_prompt_payload():
    if not layout_content or not instructions_content:
        return jsonify({"error": "Layout or instructions missing"}), 400

    prompt = f"""
You are given layout and instructions for generating mock data.

Layout:
{layout_content}

Instructions:
{instructions_content}

Task:
Return a JSON mapping of column names to faker fields (e.g., name, email, pyint, date_of_birth).

Only return valid JSON. Do NOT include explanations.
"""
    return jsonify({
        "systemInstruction": "You are a helpful assistant that generates Faker field mappings.",
        "query": prompt
    })

@app.route("/store_mapping", methods=["POST"])
def store_mapping():
    global faker_mapping
    try:
        mapping = request.json.get("mapping")
        faker_mapping = json.loads(mapping) if isinstance(mapping, str) else mapping
        return jsonify({"message": "Mapping stored", "faker_mapping": faker_mapping})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/generate", methods=["GET"])
def generate_mock_data():
    global mock_data
    try:
        n = int(request.args.get("n", 10))
        mock_data = []
        for _ in range(n):
            row = {}
            for col, method in faker_mapping.items():
                try:
                    val = getattr(faker, method)() if hasattr(faker, method) else f"<invalid:{method}>"
                    row[col] = val
                except Exception:
                    row[col] = "<error>"
            mock_data.append(row)
        return jsonify(mock_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/save", methods=["GET"])
def save_to_csv():
    global mock_data
    try:
        if not mock_data:
            return jsonify({"error": "No data to save"}), 400
        df = pd.DataFrame(mock_data)
        df.to_csv("mock_output.csv", index=False)
        return jsonify({"message": "Mock data saved to mock_output.csv"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5000)


################

# main.py
from mcp.server.fastmcp import FastMCP
import httpx
import json

mcp = FastMCP("copilot-mockgen")

API = "http://localhost:5000"

async def post(endpoint, data=None):
    async with httpx.AsyncClient() as client:
        return await client.post(f"{API}{endpoint}", json=data or {})

async def get(endpoint):
    async with httpx.AsyncClient() as client:
        return await client.get(f"{API}{endpoint}")

@mcp.tool()
async def upload(layout: str, instructions: str) -> str:
    """Upload layout and instructions."""
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{API}/upload", data={"layout": layout, "instructions": instructions})
        return res.text

@mcp.tool()
async def get_prompt() -> dict:
    """Get prompt payload for Copilot."""
    res = await get("/prompt_payload")
    return res.json()

@mcp.tool()
async def store_mapping(mapping: str) -> str:
    """Store Copilot-generated mapping JSON."""
    try:
        json.loads(mapping)  # validate first
        res = await post("/store_mapping", {"mapping": mapping})
        return res.text
    except json.JSONDecodeError:
        return "❌ Invalid JSON format."

@mcp.tool()
async def generate_mock(n: int = 10) -> list[dict]:
    """Generate mock data."""
    res = await get(f"/generate?n={n}")
    return res.json()

@mcp.tool()
async def save_to_csv() -> str:
    """Save generated mock data to CSV."""
    res = await get("/save")
    return res.text

if __name__ == "__main__":
    mcp.run()


##################
Call get_prompt to get the prompt.
Use it to generate a JSON mapping of column names to Faker methods.
Then call store_mapping with that JSON.
Then call generate_mock to generate mock data and finally call save_to_csv to store in a CSV file.

