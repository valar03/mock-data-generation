from flask import Flask, jsonify

app = Flask(__name__)

# Global in-memory store
memory_store = {
    "layout": "",
    "instructions": ""
}

@app.route("/upload-files", methods=["POST"])
def upload_files():
    try:
        with open("layout.csv", "r") as lf:
            memory_store["layout"] = lf.read()
        with open("instructions.txt", "r") as inf:
            memory_store["instructions"] = inf.read()
        return jsonify({
            "message": "Files uploaded successfully",
            "layout_length": len(memory_store["layout"]),
            "instructions_length": len(memory_store["instructions"])
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/memory", methods=["GET"])
def get_memory():
    return jsonify(memory_store)

if __name__ == "__main__":
    app.run(port=5001)


######################

from mcp.server.fastmcp import FastMCP
import httpx

mcp = FastMCP("mock-data-step1")

FLASK_URL = "http://localhost:5001"

@mcp.tool()
async def load_input_files() -> str:
    """
    Calls Flask server to load layout.csv and instructions.txt and stores in memory.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{FLASK_URL}/upload-files", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return f"✅ Files loaded! Layout: {data['layout_length']} chars, Instructions: {data['instructions_length']} chars"
            else:
                return f"❌ Failed to load files: {response.text}"
    except Exception as e:
        return f"❌ Exception occurred: {e}"

if __name__ == "__main__":
    mcp.run()
