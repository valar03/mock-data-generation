# parser_agent.py

import re

def parse_file(filepath):
    with open(filepath, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]

    # Detect delimiter or fallback to whitespace
    delimiter = "|" if "|" in lines[0] else None

    if delimiter:
        rows = [line.split(delimiter) for line in lines]
    else:
        # fallback: split on 2+ spaces or tabs
        rows = [re.split(r'\s{2,}|\t+', line) for line in lines]

    # Detect header presence: if first row contains no digits assume it's a header
    has_header = not any(char.isdigit() for char in ''.join(rows[0]))

    if has_header:
        col_names = rows[0]
        sample_rows = rows[1:]
    else:
        col_names = [f"col_{i}" for i in range(len(rows[0]))]
        sample_rows = rows

    return col_names, sample_rows