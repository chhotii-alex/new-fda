import pandas as pd
from importlib import resources

def get_admit_diagnoses(db, limit_clause):
    table = db.get_prefix() + "encounter e"
    columns = "e.mrn,  e.admit_diag_10 icd10, e.adm_dt"
    query = db.order_select_query(limit_clause, columns, table)
    return db.do_select(query)

def get_addl_diagnoses(db, limit_clause):
    table = db.get_prefix() + "DIAGNOSIS_10 d"
    join = "%sencounter e ON d.enc_num = e.enc_num" % db.get_prefix()
    columns = "e.mrn,  d.diag_cd_10 icd10, e.adm_dt"
    query = db.order_select_query(limit_clause, columns, table, join)
    return db.do_select(query)

def get_diagnoses(db, limit=None):
    if limit:
        limit_clause = db.make_limit_clause(limit)
    else:
        limit_clause = ""
    df = pd.concat([get_admit_diagnoses(db, limit_clause),
                    get_addl_diagnoses(db, limit_clause)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    return df
