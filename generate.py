import os
import json
import pandas as pd
import re
import difflib
import requests
from faker import Faker
from collections import defaultdict, Counter
from statistics import mean, stdev

faker = Faker()

# ------------------ GenAI Cache ------------------ #

class GenAICache:
    def __init__(self, path="genai_cache.json"):
        self.path = path
        self.cache = self.load()

    def load(self):
        return json.load(open(self.path)) if os.path.exists(self.path) else {}

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.cache, f, indent=2)

    def get(self, values):
        key = "|".join(values[:5])
        return self.cache.get(key)

    def set(self, values, column_name):
        key = "|".join(values[:5])
        self.cache[key] = column_name
        self.save()

# ------------------ GenAI Interface ------------------ #

class GenAIColumnInferer:
    def __init__(self, kb, genai_url, preset_id, cache_path="genai_cache.json"):
        self.kb = kb
        self.url = genai_url
        self.preset_id = preset_id
        self.cache = GenAICache(cache_path)

    def infer_column_name(self, sample_values):
        cached = self.cache.get(sample_values)
        if cached:
            return cached

        prompt = (
            "You are a data scientist helping label CSV data columns. "
            "Based on the following sample values, suggest the most appropriate column name. "
            "Return your answer as JSON in this format: {\"column_name\": \"...\"}. "
            f"Values: {json.dumps(sample_values[:10])}"
        )

        payload = {
            "fieldList": [],
            "folderIdList": [],
            "history": "",
            "modelId": "claude-3-7-sonnet@202",
            "parameters": {"max_tokens": 5000},
            "top_p": 1.0,
            "temperature": 0.0,
            "presetId": self.preset_id,
            "query": prompt,
            "systemInstruction": "Based on this context give answer",
            "useRawFileContent": False,
            "userId": ""
        }

        try:
            response = requests.post(self.url, json=payload)
            result = response.json()
            match = re.search(r'{\s*"column_name"\s*:\s*"([^"]+)"\s*}', result.get("output", ""))
            if match:
                col = match.group(1)
                self.cache.set(sample_values, col)
                return col
        except Exception as e:
            print(f"[❌] GenAI error: {e}")
        return None

    def match_against_kb(self, sample_values, genai_suggestion):
        if genai_suggestion in self.kb.columns:
            return genai_suggestion

        candidates = list(self.kb.columns.keys())
        test_string = ' '.join(sample_values[:5])
        best_score, best_match = 0, None

        for col in candidates:
            known_vals = ' '.join(list(self.kb.value_sets[col].keys())[:10])
            score = difflib.SequenceMatcher(None, test_string, known_vals).ratio()
            if score > best_score:
                best_score, best_match = score, col

        return best_match if best_score > 0.7 else genai_suggestion or None

# ------------------ Knowledge Base ------------------ #

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
        match = self._get_alias_match(name)
        if match:
            if name not in self.columns[match]:
                self.columns[match].append(name)
            return match
        self.columns[name].append(name)
        return name

    def _get_alias_match(self, name):
        for canon, aliases in self.columns.items():
            if difflib.get_close_matches(name.lower(), [canon.lower()] + [a.lower() for a in aliases], n=1, cutoff=0.85):
                return canon
        return None

    def update_patterns(self, column, values):
        detected = PatternEngine.infer_patterns(values)
        self.patterns[column] = list(set(self.patterns[column] + detected))
        self.value_sets[column].update(values)
        if len(set(values)) == len(values):
            self.uniques.add(column)
        if any(p in detected for p in ("int", "float")):
            self._update_numeric_stats(column, values)

    def _update_numeric_stats(self, column, values):
        numeric_vals = [float(v) for v in values if re.match(r"^\d+(\.\d+)?$", v)]
        if numeric_vals:
            self.stats[column] = {
                "min": min(numeric_vals),
                "max": max(numeric_vals),
                "mean": mean(numeric_vals),
                "std": stdev(numeric_vals) if len(numeric_vals) > 1 else 0,
                "max_length": max(len(str(int(v))) for v in numeric_vals)
            }

# ------------------ Pattern Engine ------------------ #

class PatternEngine:
    @staticmethod
    def infer_patterns(values):
        values = [v for v in values if v]
        if not values: return ["text"]
        checks = {
            "int": all(re.match(r"^\d+$", v) for v in values),
            "float": all(re.match(r"^\d+\.\d{2}$", v) for v in values),
            "date": all(re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}$", v) for v in values),
            "boolean": all(v.lower() in ["yes", "no", "true", "false", "y", "n"] for v in values)
        }
        for key, passed in checks.items():
            if passed: return [key]
        return ["categorical"] if len(set(values)) < 20 else ["text"]

# ------------------ Generator ------------------ #

class MockGenerator:
    def __init__(self, kb, genai, records=500):
        self.kb = kb
        self.genai = genai
        self.records = records
        self.generated_uniques = defaultdict(set)

    def parse_file(self, filepath):
        with open(filepath, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        delim = "," if "," in lines[0] else "|" if "|" in lines[0] else None
        if not delim:
            # Attempt space-based fallback
            lines = [re.split(r'\s{2,}', line) for line in lines]
            delim = "SPACE"

        if delim != "SPACE":
            first = lines[0].split(delim)
            second_row = lines[1].split(delim) if len(lines) > 1 else []
            header_likely = all(re.match(r"[A-Za-z_]{2,}", cell) for cell in first)

            if header_likely:
                columns = first
                data = [line.split(delim) for line in lines[1:]]
                return columns, data
            else:
                data = [line.split(delim) for line in lines]
                return [], data

        # SPACE-delimited fallback
        header_likely = all(re.match(r"[A-Za-z_]{2,}", val) for val in lines[0])
        if header_likely:
            return lines[0], lines[1:]
        else:
            return [], lines


    def infer_columns(self, data_rows):
        guessed = []
        for i, col_vals in enumerate(zip(*data_rows)):
            sample = list(col_vals)[:20]
            genai_guess = self.genai.infer_column_name(sample)
            final_col_name = self.genai.match_against_kb(sample, genai_guess) or f"col_{i}"
            self.kb.add_column(final_col_name)
            self.kb.update_patterns(final_col_name, sample)
            guessed.append(final_col_name)
        return guessed

    def learn(self, columns, data_rows):
        if not columns:
            return self.infer_columns(data_rows)
        canonical = []
        for i, col in enumerate(columns):
            canon = self.kb.add_column(col)
            values = [r[i] for r in data_rows if i < len(r)]
            self.kb.update_patterns(canon, values)
            canonical.append(canon)
        return canonical

    def generate_value(self, col, i):
        patterns = self.kb.patterns.get(col, [])
        stats = self.kb.stats.get(col, {})
        common = [v for v, _ in self.kb.value_sets[col].most_common(10)]
        if col in self.kb.uniques:
            if "int" in patterns:
                val = int(stats.get("min", 1000)) + len(self.generated_uniques[col])
                self.generated_uniques[col].add(val)
                return val
            if "text" in patterns:
                while True:
                    val = faker.uuid4()
                    if val not in self.generated_uniques[col]:
                        self.generated_uniques[col].add(val)
                        return val
        if "int" in patterns:
            return faker.random_int(min=int(stats.get("min", 1000)), max=int(stats.get("max", 9999)))
        if "float" in patterns:
            mu, sigma = stats.get("mean", 100.0), stats.get("std", 10.0)
            return round(faker.random.uniform(mu - sigma, mu + sigma), 2)
        if "date" in patterns:
            return faker.date()
        if "boolean" in patterns:
            return faker.random_element(["Yes", "No"])
        if common:
            return faker.random_element(common)
        return faker.word()

    def generate(self, columns):
        return [{col: self.generate_value(col, i) for col in columns} for i in range(self.records)]

# ------------------ Main ------------------ #

def main(input_file, output_file, rows, genai_url, preset_id):
    kb = KnowledgeBase()
    genai = GenAIColumnInferer(kb, genai_url, preset_id)
    gen = MockGenerator(kb, genai, rows)
    cols, data = gen.parse_file(input_file)

    if not data:
        print("[❌] Failed to parse input file. Check format or delimiter.")
        return

    if cols:
        print("[ℹ️] Header detected. Using KB only.")
    else:
        print("[ℹ️] No header detected. Using GenAI + KB for column inference.")

    canonical_cols = gen.learn(cols, data)
    mock = gen.generate(canonical_cols)
    pd.DataFrame(mock)[canonical_cols].to_csv(output_file, index=False)
    kb.save()
    print(f"[✅] {rows} mock rows written to {output_file}")

