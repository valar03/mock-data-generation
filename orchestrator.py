# orchestrator.py

import csv
import json
from agents.parser_agent import parse_file
from agents.generator_agent import generate_mock_data
from agents.matcher_agent import match_column  # optional matcher for column semantics

INPUT_PATH = "sample1.dat"
OUTPUT_PATH = "mock_output.csv"

def main():
    col_names, sample_rows = parse_file(INPUT_PATH)

    print("[INFO] Detected columns:", col_names)
    print("[INFO] Sample row(s):", sample_rows[:3])

    # Optional semantic enrichment if column names are unknown
    used_names = set()
    enriched_columns = []
    for idx, col in enumerate(col_names):
        new_col = match_column(col, sample_rows, idx)
        if new_col in used_names:
            new_col = f"{new_col}_{idx}"  # avoid collisions
        used_names.add(new_col)
        enriched_columns.append(new_col)

    mock_data = generate_mock_data(enriched_columns, sample_rows, n=500)

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(enriched_columns)
        writer.writerows(mock_data)

    print(f"[✅] Mock data written to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()