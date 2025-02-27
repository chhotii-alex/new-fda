import importlib.resources
import pandas as pd

class ResultFetch:
    def __init__(self, dx, table_name, where_clause_value, quant):
        self.dx = dx
        self.table_name = table_name
        if self.table_name == 'vwMICRO_Test':
            self.where_clause_col = 't.test_name'
            self.join_table = 'vwMICRO_Specimen_All'
            self.join_on = 's.lab_number = t.lab_number'
            self.mrn_col = 't.mrn'
            self.date_col = 's.spec_dt'
            self.result_columns = ['t.text_result', 't.comments']
        elif self.table_name == 'vwMICRO_Organisms':
            self.where_clause_col = 't.org_name'
            self.join_table = 'vwMICRO_Specimen_All' 
            self.join_on = 's.lab_number = t.lab_number'
            self.mrn_col = 's.mrn'
            self.date_col = 's.spec_dt'
            self.result_columns = ['t.quantity', 't.comments']
        elif self.table_name == 'vwLAB_Result':
            self.where_clause_col = 't.result_name'
            self.join_table = 'vwLAB_Specimen'
            self.join_on = 't.specimen_id = s.specimen_id'
            self.mrn_col = 't.mrn'
            self.date_col = 's.specimen_dt'
            if quant:
                self.result_columns = ['t.result_value_num', 't.units', 't.comments']
            else:
                self.result_columns = ['t.result_value', 't.units', 't.comments']
        self.where_clause_value = where_clause_value
        self.quantitative = quant

    def make_query(self, db, limit=None):
        result_cols = ', '.join(self.result_columns)
        table_prefix = db.get_prefix()
        limit_clause = db.make_limit_clause(limit)
        return db.order_select_query(
            limit_clause,
            f"""{self.mrn_col} mrn, {self.date_col} dx_date, {result_cols}, s.gender, s.dob, s.pat_type_full """,
            f"""{table_prefix}{self.table_name} t""",
            f""" {table_prefix}{self.join_table} s on {self.join_on} """,
            f"""{self.where_clause_col} = '{self.where_clause_value}'""")
    
    def make_count_query(self, db, limit=None):
        result_cols = ', '.join(self.result_columns)
        return_cols = 'count(*) row_count, ' + result_cols 
        order_cols =  'row_count DESC, ' + result_cols 
        table_prefix = db.get_prefix()
        limit_clause = db.make_limit_clause(limit)
        return db.order_select_query(
            limit_clause,
            return_cols,
            f"{table_prefix}{self.table_name} t",
            f"{table_prefix}{self.join_table} s on {self.join_on}",
            f"""{self.where_clause_col} = '{self.where_clause_value}'""",
            result_cols, 
            order_cols
        )

    
    def get_df_col(self, sql_col):
        fields = sql_col.split(".")
        if len(fields) == 2:
            return fields[1]
        elif len(fields) == 1:
            return sql_col
        else:
            raise Exception("how is this formatted")
        
    def get_date_col(self):
        return self.get_df_col(self.date_col)
    
    def get_result_columns(self):
        return [self.get_df_col(col) for col in self.result_columns]

def get_queries():
    with importlib.resources.open_text("new_fda", "labval_queries.csv") as f:
        df = pd.read_csv(f)
    df['is_quant'] = df['Quantitative?'] == 'y'
    df['where_val'] = df['where clause value']
    queries = [ResultFetch(row[1].dx, row[1].table, row[1].where_val, row[1].is_quant) for row in df.iterrows()]
    return queries
