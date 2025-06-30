import random
from faker import Faker
from collections import defaultdict

faker = Faker()

class MockDataGenerator:
    def __init__(self, kb, records=500):
        self.kb = kb
        self.records = records
        self.generated_uniques = defaultdict(set)

    def generate_value(self, col, i):
        patterns = self.kb.patterns.get(col, [])
        stats = self.kb.stats.get(col, {})
        common_vals = [v for v, _ in self.kb.value_sets[col].most_common(10)]

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
            mu = stats.get("mean", 100.0)
            sigma = stats.get("std", 10.0)
            return round(random.uniform(mu - sigma, mu + sigma), 2)
        if "date" in patterns:
            return faker.date()
        if "boolean" in patterns:
            return faker.random_element(["Yes", "No"])
        if common_vals:
            return faker.random_element(common_vals)
        return faker.word()

    def generate(self, columns):
        data = []
        for i in range(self.records):
            row = {col: self.generate_value(col, i) for col in columns}
            data.append(row)
        return data
