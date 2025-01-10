
class ResultFetch:
    def __init__(self, dx, table_name, mrn_col="mrn", date_col="test_final_dt", text_col="comments", where_clause=""):
        self.dx = dx
        self.table_name = table_name
        self.mrn_col = mrn_col
        self.date_col = date_col
        self.text_col = text_col
        self.where_clause = where_clause

    def make_query(self):
        return """SELECT %s, %s, %s FROM %s 
            WHERE %s""" % (self.mrn_col, self.date_col, self.text_col, self.table_name, self.where_clause)


queries_by_dx = {
        "RUBELLA" : [
            ResultFetch("")
        ],
        'VARICELLA-ZOSTER': [],
        'CLOSTRIDIUM DIFFICILE' : [],
        'HELIOBACTER PYLORI' : [],
        'RUBEOLA' : [],
        'HIV-1' : [],
        'HCV' : [],
        'BETA STREP GROUP A' : [],
        'HBC' : [],
        'MUMPS' : [],
        'LYME' : [],
        'CYPTOSPORIDIUM' : [],
        'CMV' : [],
}