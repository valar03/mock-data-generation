# generator_agent.py

import random
import re
import json
import os
from faker import Faker
from collections import Counter, defaultdict

fake = Faker()
KB_PATH = "kb/financial_kb_us.json"

def load_kb():
    if os.path.exists(KB_PATH):
        with open(KB_PATH) as f:
            return json.load(f)
    return {}

def save_kb(kb):
    with open(KB_PATH, "w") as f:
        json.dump(kb, f, indent=2)

def infer_data_type_from_samples(values):
    values = [v.strip() for v in values if v.strip()]
    if not values:
        return "text"

    if all(re.match(r"^-?\d+$", v) for v in values):
        return "integer"
    if all(re.match(r"^-?\d+[.,]?\d*$", v) for v in values):
        return "float"
    if all(v.upper() in {"Y", "N"} for v in values):
        return "boolean"
    if all(re.match(r"\d{4}-\d{2}-\d{2}", v) for v in values):
        return "iso_date"
    if len(set(values)) <= 20 and all(len(v.split()) <= 6 for v in values):
        return "enum"
    return "text"

GENERATOR_MAP = defaultdict(lambda: lambda: fake.word())
GENERATOR_MAP.update({
    "boolean": lambda: random.choice(["Y", "N"]),
    "integer": lambda: str(fake.random_int(min=1, max=9999)),
    "float": lambda: f"{random.uniform(1.0, 100000.0):.2f}",
    "iso_date": lambda: str(fake.date_this_decade()),
    "text": lambda: fake.word(),
    "long_numeric_id": lambda: str(fake.random_number(digits=14)),
    "alphanumeric": lambda: fake.bothify(text="??##??##"),
    "money": lambda: f"{random.uniform(100, 999999):,.2f}",
    "text_code": lambda: fake.lexify(text="???? ??"),
    "account_number": lambda: str(fake.random_number(digits=12)),
    "transaction_id": lambda: fake.bothify(text="TXN-####-??"),
    "branch_code": lambda: fake.bothify(text="BR-??##"),
    "status_flag": lambda: random.choice(["Y", "N"]),
    "transaction_amount": lambda: f"{random.uniform(1.0, 10000.0):.2f}"
})

def generate_mock_data(col_names, sample_rows, n=500):
    kb = load_kb()
    dtype_map = {}
    enum_candidates = {}

    for i, col in enumerate(col_names):
        values = [row[i].strip() for row in sample_rows if len(row) > i and row[i].strip()]
        prev_dtype = kb.get(col, {}).get("data_type")

        inferred_type = infer_data_type_from_samples(values)
        dtype_map[col] = inferred_type
        kb[col] = kb.get(col, {"aliases": [col]})
        kb[col]["data_type"] = inferred_type

        if inferred_type == "enum":
            freq = Counter(values)
            top_enums = [v for v, _ in freq.most_common(20)]
            kb[col]["enum_values"] = top_enums
            enum_candidates[col] = top_enums

    save_kb(kb)

    rows = []
    for _ in range(n):
        row = []
        for col in col_names:
            dtype = dtype_map.get(col, "text")
            if dtype == "enum":
                val = random.choice(kb[col]["enum_values"])
            else:
                val = GENERATOR_MAP[dtype]()
            row.append(val)
        rows.append(row)

    return rows