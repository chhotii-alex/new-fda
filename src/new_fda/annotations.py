
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


def get_annotation_queries():
    return [SheetFetch(result) for result in [
        'Tobacco',
    ]
    ]
