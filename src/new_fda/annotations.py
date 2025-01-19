import re

"""
There's also the pack years sheet result.
Do we distinguish between current and former? Often
it's unclear.
"""
def parse_tobacco_usage(comment):
    comment = comment.strip().upper()
    if not len(comment):
        return None
    if "UNKNOWN" in comment:
        return None
    if "CURRENT" in comment:
        return "current"
    if "EVERY DAY" in comment:
        return "current"
    if "SOME DAY" in comment:
        return "current"
    if "LIGHT" in comment:
        return "current"
    if "LESS THAN 10" in comment:
        return "current"
    if "FORMER" in comment:
        return "former"
    if "HEAVY" in comment:
        return "current"
    if "10+" in comment:
        return "current"
    if "YES" == comment[:3]:
        return "current"
    if "NEVER" in comment:
        return "never"
    if "NO" == comment[:2]:
        return "never"
    if "COUNSELING PROVIDED" in comment:
        return "current"
    print(comment)
    return None

starts_with_float_re = re.compile(r"(\d+\.?\d*)(.*)")

def parse_starting_num(value):
    if value is None:
        return None
    m = starts_with_float_re.match(value)
    if m:
        return float(m[1])
    return None

def parse_bmi(value):
    return parse_starting_num(value)

def parse_weight(value):
    """
    ASSUMING that the units is ALWAYS pounds
    """
    return parse_starting_num(value)

def parse_height(str_val):
    """
    Often we have to guess how to interpret this.
    Returns height in INCHES.
    """
    m = re.match(r"~?(\d+)\'\s*(\d+\.?\d*)", str_val)
    if m:
        feet = float(m.group(1))
        inches = float(m.group(2))
        val = feet * 12 + inches
        return val
    m = re.match(r"~?(\d+)\'", str_val)
    if m:
        feet = float(m.group(1))
        val = feet * 12
        return val
    m = re.match(r"(\d+)ft(\d*\.?\d*)(in)?", str_val, re.IGNORECASE)
    if m:
        feet = float(m.group(1))
        inches = float(m.group(2))
        val = feet * 12 + inches
        return val
    m = re.match(r"(\d+)\s*cm\.?", str_val, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return val / 2.54
    m = re.match(r"(\d+)\s*lbs\.?", str_val, re.IGNORECASE)
    if m:
        return None
    m = re.match(r"(\d+)\s*in\.?", str_val, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return val
    m = re.match(r"(\d)/(\d+)", str_val)
    if m:
        feet = int(m.group(1))
        inches = int(m.group(2))
        return feet * 12 + inches
    m = re.match(r"~?(\d+\.?\d*)", str_val)
    if m:
        val = float(m.group(1))
        if val < 9:  # Feet?
            return val * 12
        if val < 97:
            return val
        elif val < 120:  # guess that it's weight in lbs.
            return None
        else:  # could be height in cm
            return (val / 2.54)
    return None    

class SheetFetch:
    def __init__(self, result_name):
        self.result_name = result_name

    def make_query(self, db, limit=None):
        result_cols = """mrn, result_value, result_dt"""
        table_prefix = db.get_prefix()
        limit_clause = db.make_limit_clause(limit)
        return db.order_select_query(
            limit_clause,
            result_cols,
            f"""{table_prefix}vwOMR_Sheet_Result""",
            None,
            f"""result_name = '{self.result_name}'""")

parsers = {'Tobacco': parse_tobacco_usage,
           "BMI (kg/m2)": parse_bmi,
    "Bariatric BMI (kg/m2)": parse_bmi,
    "BMI": parse_bmi,
    "Bariatric BMI": parse_bmi,
    "Weight": parse_weight,
    "Weight (Lbs)": parse_weight,
    "Height (Inches)": parse_height,
    }

def get_annotation_queries():
    return [(SheetFetch(item[0]), item[1]) for item in parsers.items()]
