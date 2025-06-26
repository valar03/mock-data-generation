import re

def guess_column_semantics(values):
    values = [v.strip() for v in values if v.strip()]
    if not values:
        return ("text", "text")

    if all(re.fullmatch(r"\d{12,20}", v) for v in values):
        return ("account_number", "long_numeric_id")
    if all(re.fullmatch(r"[A-Z0-9]{4,10}", v) for v in values):
        return ("transaction_id", "alphanumeric")
    if all(re.fullmatch(r"\d{1,3}\.\d{3,5}\.\d{2}", v) for v in values):
        return ("transaction_amount", "money")
    if all(re.fullmatch(r"[A-Z]{4,10}( [A-Z]{2,5})?", v) for v in values):
        return ("branch_code", "text_code")
    if all(v.upper() in {"Y", "N"} for v in values):
        return ("status_flag", "boolean")
    return ("text", "text")
