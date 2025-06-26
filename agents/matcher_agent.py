# agents/matcher_agent.py

import json
import os
from agents.semantic_column_guesser import guess_column_semantics

KB_PATH = "kb/financial_kb_us.json"

def load_kb():
    if os.path.exists(KB_PATH):
        with open(KB_PATH) as f:
            return json.load(f)
    return {}

def save_kb(kb):
    with open(KB_PATH, "w") as f:
        json.dump(kb, f, indent=2)

def match_column(col_name, sample_rows, col_index):
    kb = load_kb()
    column_values = [row[col_index].strip() for row in sample_rows if len(row) > col_index]

    # KB match
    for canonical, entry in kb.items():
        if col_name in entry.get("aliases", []):
            return canonical

    # Semantic guess
    guessed_name, guessed_type = guess_column_semantics(column_values)
    if guessed_name not in kb:
        kb[guessed_name] = {
            "aliases": [col_name],
            "data_type": guessed_type
        }
        save_kb(kb)

    return guessed_name
