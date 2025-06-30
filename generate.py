import os
import json
import pandas as pd
from faker import Faker
from collections import defaultdict, Counter
import re
import difflib
from statistics import mean, stdev

faker = Faker()

# ------------------- Knowledge Base -------------------

class KnowledgeBase:
    def __init__(self, path="knowledge_base.json"):
        self.path = path
        self.columns = defaultdict(list)
        self.patterns = defaultdict(list)
        self.value_sets = defaultdict(Counter)
        self.stats = defaultdict(dict)
        self.uniques = set()
        self.load()

    def load(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                data = json.load(f)
                self.columns = defaultdict(list, data.get("columns", {}))
                self.patterns = defaultdict(list, data.get("patterns", {}))
                self.value_sets = defaultdict(Counter, {
                    k: Counter(v) for k, v in data.get("value_sets", {}).items()
                })
                self.stats = defaultdict(dict, data.get("stats", {}))
                self.uniques = set(data.get("uniques", []))

    def save(self):
        with open(self.path, "w") as f:
            json.dump({
                "columns": self.columns,
                "patterns": self.patterns,
                "value_sets": {k: dict(v) for k, v in self.value_sets.items()},
                "stats": self.stats,
                "uniques": list(self.uniques)
            }, f, indent=2)

    def get_alias_match(self, name):
        for canon in self.columns.keys():
            matches = difflib.get_close_matches(name.lower(), [canon.lower()] + [a.lower() for a in self.columns[canon]], n=1, cutoff=0.85)
            if matches:
                return canon
        return None

    def add_column(self, name):
        alias_match = self.get_alias_match(name)
        if alias_match:
            if name not in self.columns[alias_match]:
                self.columns[alias_match].append(name)
            return alias_match
        self.columns[name].append(name)
        return name

    def update_patterns(self, column, values):
        detected = PatternEngine.infer_patterns(values)
        self.patterns[column] = list(set(self.patterns[column] + detected))
        self.value_sets[column].update(values)

        if len(set(values)) == len(values):
            self.uniques.add(column)

        if "int" in detected or "float" in detected:
            numeric_vals = [float(v) for v in values if re.match(r"^\d+(\.\d+)?$", v)]
            if numeric_vals:
                self.stats[column] = {
                    "min": min(numeric_vals),
                    "max": max(numeric_vals),
                    "mean": mean(numeric_vals),
                    "std": stdev(numeric_vals) if len(numeric_vals) > 1 else 0,
                    "max_length": max(len(str(int(v))) for v in numeric_vals)
                }

# ------------------- Pattern Engine -------------------

class PatternEngine:
    @staticmethod
    def infer_patterns(values):
        values = [v for v in values if v]
        patterns = set()

        if all(PatternEngine.is_int(v) for v in values):
            patterns.add("int")
        elif all(PatternEngine.is_float(v) for v in values):
            patterns.add("float")
        elif all(PatternEngine.is_date(v) for v in values):
            patterns.add("date")
        elif all(v.lower() in ["yes", "no", "true", "false", "y", "n"] for v in values):
            patterns.add("boolean")
        elif len(set(values)) < 20:
            patterns.add("categorical")
        else:
            patterns.add("text")

        return list(patterns)

    @staticmethod
    def is_int(value):
        return re.match(r"^\d+$", value)

    @staticmethod
    def is_float(value):
        return re.match(r"^\d+\.\d{2}$", value)

    @staticmethod
    def is_date(value):
        return re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}$", value)

# ------------------- Generator -------------------

class MockGenerator:
    def __init__(self, kb: KnowledgeBase, records=500):
        self.kb = kb
        self.records = records
        self.generated_uniques = defaultdict(set)

    def parse_file(self, filepath):
        with open(filepath, "r") as f:
            lines = f.readlines()

        delim = "," if "," in lines[0] else "|" if "|" in lines[0] else None
        if not delim:
            return None, None

        first = lines[0].strip().split(delim)
        header_likely = any(re.match(r'[A-Za-z_]+', cell) for cell in first)

        if header_likely:
            columns = first
            data = [l.strip().split(delim) for l in lines[1:] if delim in l]
        else:
            columns = []
            data = [l.strip().split(delim) for l in lines if delim in l]

        return columns, data

    def infer_columns(self, data_rows):
        transposed = list(zip(*data_rows))
        guessed = []
        for i, col_vals in enumerate(transposed):
            sample = list(col_vals)[:20]
            existing = self.match_existing_pattern(sample)
            col_name = existing if existing else f"col_{i}"
            self.kb.add_column(col_name)
            self.kb.update_patterns(col_name, sample)
            guessed.append(col_name)
        return guessed

    def match_existing_pattern(self, values):
        for col, pattern in self.kb.patterns.items():
            sample_pattern = PatternEngine.infer_patterns(values)
            if set(pattern) & set(sample_pattern):
                return col
        return None

    def learn(self, columns, data_rows):
        if columns:
            canonical = []
            for i, col in enumerate(columns):
                canon = self.kb.add_column(col)
                values = [r[i] for r in data_rows if i < len(r)]
                self.kb.update_patterns(canon, values)
                canonical.append(canon)
            return canonical
        else:
            return self.infer_columns(data_rows)

    def generate_value(self, col, i):
        patterns = self.kb.patterns.get(col, [])
        stats = self.kb.stats.get(col, {})
        common_values = [v for v, c in self.kb.value_sets[col].most_common(10)]

        if col in self.kb.uniques:
            if "int" in patterns:
                base = int(stats.get("min", 1000))
                while base in self.generated_uniques[col]:
                    base += 1
                self.generated_uniques[col].add(base)
                return base
            elif "text" in patterns:
                val = faker.uuid4()
                while val in self.generated_uniques[col]:
                    val = faker.uuid4()
                self.generated_uniques[col].add(val)
                return val

        if "int" in patterns:
            return faker.random_int(min=int(stats.get("min", 1000)), max=int(stats.get("max", 9999)))
        if "float" in patterns:
            mu = stats.get("mean", 100.0)
            sigma = stats.get("std", 10.0)
            return round(faker.random.uniform(mu - sigma, mu + sigma), 2)
        if "date" in patterns:
            return faker.date()
        if "boolean" in patterns:
            return faker.random_element(["Yes", "No"])
        if "categorical" in patterns and common_values:
            return faker.random_element(common_values)
        if "text" in patterns and common_values:
            return faker.random_element(common_values)
        return faker.word()

    def generate(self, columns):
        output = []
        for i in range(self.records):
            row = {col: self.generate_value(col, i) for col in columns}
            output.append(row)
        return output

# ------------------- Main Script -------------------

def main(input_file="sample1.dat", output_file="mock_output_final.csv", rows=500):
    kb = KnowledgeBase()
    gen = MockGenerator(kb, rows)
    cols, data = gen.parse_file(input_file)

    if not data:
        print("[❌] Failed to parse input.")
        return

    canonical_cols = gen.learn(cols, data)
    mock = gen.generate(canonical_cols)

    pd.DataFrame(mock)[canonical_cols].to_csv(output_file, index=False)
    kb.save()
    print(f"[✅] {rows} mock rows written to {output_file}")

# ------------------- Run -------------------

if __name__ == "__main__":
    main("sample1.dat", "mock_output_final.csv", 500)
