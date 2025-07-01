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
        self.columns = defaultdict(list)       # canonical -> aliases
        self.patterns = defaultdict(list)      # column -> patterns
        self.value_sets = defaultdict(Counter) # column -> value frequency
        self.stats = defaultdict(dict)         # column -> numeric stats
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

# -------- Delimiter + Header Detection -------
def detect_delim_and_header(filepath):
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) < 1: return None, False, []

    # Detect delimiter
    test_line = lines[0]
    if "," in test_line:
        delim = ","
    elif "|" in test_line:
        delim = "|"
    elif "\t" in test_line:
        delim = "\t"
    else:
        delim = None  # fallback to whitespace

    # Tokenize lines
    splitter = lambda l: l.split(delim) if delim else re.split(r"\s{2,}|\t+", l.strip())
    split_lines = [splitter(l) for l in lines]

    if len(split_lines) < 2:
        return delim, False, split_lines

    first, second = split_lines[0], split_lines[1]
    is_text = lambda x: bool(re.fullmatch(r"[A-Za-z_]+", x))
    is_num = lambda x: bool(re.fullmatch(r"\d+(\.\d+)?", x))

    header_confidence = sum(is_text(c) for c in first) / len(first) > 0.5
    row_confidence = sum(is_num(c) for c in second) / len(second) > 0.3

    has_header = header_confidence and row_confidence
    return delim, has_header, split_lines
    

# -------- Main --------
def run_pipeline(input_file, output_file, record_count):
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} does not exist.")
    filepath = "../sample1.dat"
    delim, has_header, lines = detect_delim_and_header(filepath)
    if delim is None and not has_header:
        print("❌ Could not detect delimiter or header")
        return

    kb = KnowledgeBase()
    rows = [line.split(delim) if delim else re.split(r"\s{2,}", line.strip()) for line in lines]
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
        sample_rows = rows[:3]  # Limit to 2-3 rows max for GenAI
        final_headers = infer_headers_using_genai(sample_rows, kb)

        for i, col in enumerate(final_headers):
            values = [r[i] for r in rows if i < len(r)]
            canon = kb.add_column(col)
            kb.update_patterns(canon, values)
        kb.save()
        print("✅ Knowledge Base updated using GenAI-inferred headers.")

    print(f"🎯 Generating {record_count} mock records based on resolved columns.")
    generator = MockDataGenerator(kb, record_count)
    mock_data = generator.generate(final_headers)

    pd.DataFrame(mock_data)[final_headers].to_csv(output_file, index=False)
    print(f"✅ Mock data written to {output_file}")

if __name__ == "__main__":
    run_pipeline("../sample1.dat", "mock_output_final.csv", 500)
