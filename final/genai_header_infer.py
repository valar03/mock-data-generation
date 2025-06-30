import json
import difflib
import requests

def call_genai_prompt(sample_rows):
    prompt = f"""You are a helpful assistant that understands tabular data. Based on the rows below, infer what each column likely represents.

Return your result as JSON with key "columns" like:
{{ "columns": ["Account Number", "Branch Code", "IFSC", "Amount", "Address"] }}

Data:
{json.dumps(sample_rows, indent=2)}
"""
    payload = {
        "fieldList": [],
        "folderIdList": [],
        "history": "",
        "modelId": "claude-3-7-sonnet@202",
        "parameters": {"max_tokens": 1500},
        "top_p": 1.0,
        "temperature": 0.0,
        "presetId": "your-preset-id",
        "query": prompt,
        "systemInstruction": "You are a data assistant that only returns JSON response with column headers.",
        "useRawFileContent": False,
        "userId": ""
    }

    response = requests.post("https://your-genai-api-endpoint.com/generate", json=payload)
    result = response.json()

    try:
        return json.loads(result["text"])["columns"]
    except Exception as e:
        print(f"❌ Error parsing GenAI response: {e}")
        return []

def infer_headers_using_genai(sample_rows, kb):
    genai_columns = call_genai_prompt(sample_rows)
    final_headers = []

    for col_idx, genai_col in enumerate(genai_columns):
        col_values = [row[col_idx] for row in sample_rows if col_idx < len(row)]
        col_values_clean = [v.strip().lower() for v in col_values if v.strip()]

        # 1️⃣ Try value set similarity
        value_match = None
        best_val_score = 0
        for kb_col, counter in kb.value_sets.items():
            kb_vals = ' '.join(list(counter.keys())[:10]).lower()
            input_vals = ' '.join(col_values_clean)
            score = difflib.SequenceMatcher(None, input_vals, kb_vals).ratio()
            if score > best_val_score:
                best_val_score = score
                value_match = kb_col
        if best_val_score > 0.85:
            print(f"🔁 [Value Match] {genai_col} → {value_match} (score: {best_val_score:.2f})")
            final_headers.append(value_match)
            continue

        # 2️⃣ Try column name alias match
        alias_match = None
        best_name_score = 0
        for kb_col, aliases in kb.columns.items():
            all_names = [kb_col] + aliases
            for alias in all_names:
                score = difflib.SequenceMatcher(None, genai_col.lower(), alias.lower()).ratio()
                if score > best_name_score:
                    best_name_score = score
                    alias_match = kb_col
        if best_name_score > 0.85:
            print(f"🔁 [Alias Match] {genai_col} → {alias_match} (score: {best_name_score:.2f})")
            final_headers.append(alias_match)
            continue

        # 3️⃣ Fallback to GenAI output
        print(f"📎 [Fallback] Using GenAI column: {genai_col}")
        final_headers.append(genai_col)

    return final_headers
