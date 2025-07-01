import os
import json
import re
from collections import defaultdict, Counter
from statistics import mean, stdev
from genai_header_infer import infer_headers_using_genai  
from mock_generator import MockDataGenerator
import pandas as pd

# -------- Knowledge Base --------
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
        if not os.path.exists(self.path): return
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

# -------- Pattern Inference --------
class PatternEngine:
    @staticmethod
    def infer(values):
        if not values: return ["text"]
        if all(re.fullmatch(r"\d+", v) for v in values): return ["int"]
        if all(re.fullmatch(r"\d+\.\d{2}", v) for v in values): return ["float"]
        if all(re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", v) for v in values): return ["date"]
        if all(v.lower() in ["yes", "no", "true", "false", "y", "n"] for v in values): return ["boolean"]
        return ["categorical"] if len(set(values)) < 20 else ["text"]

# -------- Smart Splitter --------
def smart_split(line, delim):
    if delim:
        return [cell.strip() for cell in line.split(delim)]
    return re.split(r"\s{2,}|\t+", line.strip())

# -------- Delimiter + Header Detection --------
def detect_delim_and_header(filepath):
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines: return None, False, []

    # Try to detect delimiter
    if "," in lines[0]: delim = ","
    elif "|" in lines[0]: delim = "|"
    else: delim = None

    tokens = lambda l: smart_split(l, delim)
    row1 = tokens(lines[0])
    row2 = tokens(lines[1]) if len(lines) > 1 else []

    is_text = lambda x: bool(re.fullmatch(r"[A-Za-z_][\w\s]*", x))
    is_num = lambda x: bool(re.fullmatch(r"\d+(\.\d+)?", x))

    header = (
        len(row2) > 0 and 
        sum(is_text(cell) for cell in row1) / len(row1) > 0.5 and
        sum(is_num(cell) for cell in row2) / len(row2) > 0.3
    )

    return delim, header, lines

# -------- Main --------
def run_pipeline(input_file, output_file, record_count):
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} does not exist.")

    delim, has_header, lines = detect_delim_and_header(input_file)

    if not lines:
        print("❌ Input file is empty.")
        return

    if delim is None and not has_header:
        print("❌ Could not detect delimiter or header")
        return

    kb = KnowledgeBase()
    rows = [smart_split(line, delim) for line in lines]

    if has_header:
        final_headers = rows[0]
        print(f"✅ Header detected: {final_headers}")
        for i, col in enumerate(final_headers):
            col_vals = [r[i] for r in rows[1:] if i < len(r)]
            canon = kb.add_column(col)
            kb.update_patterns(canon, col_vals)
        kb.save()
        print("✅ Knowledge base updated with header data.")
    else:
        print("🔎 Headerless file detected. Using GenAI + KB to infer headers.")
        sample_rows = rows[:3]
        final_headers = infer_headers_using_genai(sample_rows, kb)
        for i, col in enumerate(final_headers):
            values = [r[i] for r in rows if i < len(r)]
            canon = kb.add_column(col)
            kb.update_patterns(canon, values)
        kb.save()
        print("✅ Knowledge base updated using GenAI-inferred headers.")

    print(f"🎯 Generating {record_count} mock records based on resolved columns.")
    generator = MockDataGenerator(kb, record_count)
    mock_data = generator.generate(final_headers)

    pd.DataFrame(mock_data)[final_headers].to_csv(output_file, index=False)
    print(f"✅ Mock data written to {output_file}")

# ------------------- Run -------------------
if __name__ == "__main__":
    run_pipeline("../sample1.dat", "mock_output_final.csv", 500)
