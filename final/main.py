import os
import re
import json
import pandas as pd
from collections import defaultdict, Counter
from statistics import mean, stdev
from genai_header_infer import infer_headers_using_genai
from mock_generator import MockDataGenerator

# ------------ Pattern Detection ------------
class PatternEngine:
    @staticmethod
    def infer(values):
        if not values: return ["text"]
        if all(re.fullmatch(r"\d+", v) for v in values): return ["int"]
        if all(re.fullmatch(r"\d+\.\d{2}", v) for v in values): return ["float"]
        if all(re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", v) for v in values): return ["date"]
        if all(v.lower() in ["yes", "no", "true", "false", "y", "n"] for v in values): return ["boolean"]
        return ["categorical"] if len(set(values)) < 20 else ["text"]

# ------------ Knowledge Base ------------
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
                self.value_sets = defaultdict(Counter, {k: Counter(v) for k, v in data.get("value_sets", {}).items()})
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

    def add_column(self, name):
        canon = self.get_canonical(name)
        if canon:
            if name not in self.columns[canon]:
                self.columns[canon].append(name)
            return canon
        self.columns[name].append(name)
        return name

    def get_canonical(self, name):
        for canon, aliases in self.columns.items():
            if name.lower() == canon.lower() or name.lower() in [a.lower() for a in aliases]:
                return canon
        return None

    def update_patterns(self, column, values):
        values = [v for v in values if v]
        patterns = PatternEngine.infer(values)
        self.patterns[column] = list(set(self.patterns[column] + patterns))
        self.value_sets[column].update(values)

        if len(set(values)) == len(values):
            self.uniques.add(column)

        if "int" in patterns or "float" in patterns:
            nums = [float(v) for v in values if re.match(r"^\d+(\.\d+)?$", v)]
            if nums:
                self.stats[column] = {
                    "min": min(nums),
                    "max": max(nums),
                    "mean": mean(nums),
                    "std": stdev(nums) if len(nums) > 1 else 0,
                    "max_length": max(len(str(int(n))) for n in nums)
                }

# ------------ Smart Delimiter + Header Detection ------------
def smart_detect_and_split(filepath):
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        return None, False, []

    for delim in [",", "|", "\t"]:
        row1 = lines[0].split(delim)
        if len(row1) > 1:
            row2 = lines[1].split(delim) if len(lines) > 1 else []
            is_text = lambda x: bool(re.fullmatch(r"[A-Za-z_][\w\s]*", x))
            is_num = lambda x: bool(re.fullmatch(r"\d+(\.\d+)?", x))
            header = (
                len(row2) > 0 and 
                sum(is_text(c.strip()) for c in row1) / len(row1) > 0.5 and
                sum(is_num(c.strip()) for c in row2) / len(row2) > 0.3
            )
            parsed = [line.split(delim) for line in lines]
            return delim, header, parsed

    # Fallback: space-separated (only if no other delimiter works)
    if re.search(r"\s{2,}", lines[0]):
        rows = [re.split(r"\s{2,}", line.strip()) for line in lines]
        return None, False, rows

    return None, False, []

# ------------ Main Pipeline ------------
def run_pipeline(input_file, output_file, record_count):
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} not found")

    delim, has_header, rows = smart_detect_and_split(input_file)
    if not rows:
        print("❌ Could not find delimiter or parse rows")
        return

    kb = KnowledgeBase()
    if has_header:
        final_headers = rows[0]
        print(f"✅ Detected headers: {final_headers}")
        for i, col in enumerate(final_headers):
            col_vals = [r[i] for r in rows[1:] if i < len(r)]
            canon = kb.add_column(col)
            kb.update_patterns(canon, col_vals)
        kb.save()
        print("✅ Knowledge base updated with header values.")
    else:
        print("🔍 No headers found. Invoking GenAI to infer headers...")
        sample_rows = rows[:3]
        final_headers = infer_headers_using_genai(sample_rows, kb)
        for i, col in enumerate(final_headers):
            col_vals = [r[i] for r in rows if i < len(r)]
            canon = kb.add_column(col)
            kb.update_patterns(canon, col_vals)
        kb.save()
        print("✅ Knowledge base updated using GenAI inferred headers.")

    print(f"📦 Generating {record_count} mock records...")
    generator = MockDataGenerator(kb, record_count)
    mock_data = generator.generate(final_headers)
    pd.DataFrame(mock_data)[final_headers].to_csv(output_file, index=False)
    print(f"✅ Mock data written to {output_file}")

# ------------ Run ------------
if __name__ == "__main__":
    run_pipeline("sample1.dat", "mock_output_final.csv", 500)
