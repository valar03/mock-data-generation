from mcp.server.fastmcp import FastMCP
import httpx
import json

mcp = FastMCP("mock-data")
FLASK_URL = "http://localhost:5001"

@mcp.tool()
async def upload_files() -> str:
    """
    Upload layout.csv and instructions.txt contents to Flask in-memory.
    """
    try:
        with open("layout.csv", "r") as lf, open("instructions.txt", "r") as inf:
            layout_content = lf.read()
            instructions_content = inf.read()

        async with httpx.AsyncClient() as client:
            res = await client.post(
                f"{FLASK_URL}/upload-files",
                json={"layout": layout_content, "instructions": instructions_content}
            )
            return "✅ Files uploaded and stored successfully." if res.status_code == 200 else f"❌ Error: {res.text}"
    except Exception as e:
        return f"❌ Failed to upload files: {e}"

@mcp.tool()
async def capture_faker_mapping(response: str) -> str:
    """
    Takes Copilot's JSON response (as string) and stores it in Flask memory.
    Prompt in Copilot: 
    "Give a JSON mapping of column names to Faker methods based on the uploaded layout and instructions."
    """
    try:
        mapping = json.loads(response)
        async with httpx.AsyncClient() as client:
            res = await client.post(f"{FLASK_URL}/store-faker-mapping", json=mapping)
            return "✅ Mapping stored successfully!" if res.status_code == 200 else f"❌ Flask error: {res.text}"
    except Exception as e:
        return f"❌ Failed to store mapping: {e}"

if __name__ == "__main__":
    mcp.run()

#######################

from flask import Flask, request, jsonify

app = Flask(__name__)
memory_store = {}

@app.route("/upload-files", methods=["POST"])
def upload_files():
    data = request.get_json()
    layout = data.get("layout")
    instructions = data.get("instructions")

    if not layout or not instructions:
        return jsonify({"error": "Missing layout or instructions"}), 400

    memory_store["layout"] = layout
    memory_store["instructions"] = instructions
    print("✅ Uploaded layout and instructions")
    return jsonify({"message": "Files stored"}), 200

@app.route("/store-faker-mapping", methods=["POST"])
def store_faker_mapping():
    mapping = request.get_json()
    if not isinstance(mapping, dict):
        return jsonify({"error": "Invalid JSON"}), 400
    memory_store["faker_mapping"] = mapping
    print("✅ Stored faker mapping:", mapping)
    return jsonify({"message": "Mapping stored"}), 200

@app.route("/get-faker-mapping", methods=["GET"])
def get_faker_mapping():
    return jsonify(memory_store.get("faker_mapping", {})), 200

if __name__ == "__main__":
    app.run(port=5001)
