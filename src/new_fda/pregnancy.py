import re
import pandas as pd

def make_delivery_query(db):
    columns = ', '.join(['Enc_num',
               'adm_dt',  
               'delivery_type',
               'delivery_type_full',
               'hosp_svc', 
               'diagnosis',
               'mrn',
               'baby_deliver_tm'
               ])
    table = 'vwADT_Admissions'
    join = None
    where = 'baby_deliver_tm is not null'

    limit_clause = db.make_limit_clause(2000) # TODO remove
    limit_clause = None
    return db.order_select_query(
        limit_clause,
        columns,
        table,
        join,
        where)

weeks_search = re.compile(r"([234][0-9]).*WKS")
def parse_delivery_weeks(diagnosis):
    if diagnosis:
        m = weeks_search.match(diagnosis)
        if m:
            return int(m.group(1))
        if "PRETERM" in diagnosis:
            return 24
    return 40

def get_delivery_records(db):
    query = make_delivery_query(db)
    results = db.do_select(query)
    results['weeks'] = results['diagnosis'].apply(parse_delivery_weeks)
    results['start_date'] = results["baby_deliver_tm"] + pd.Timedelta(days=-7) * results['weeks']
    results['end_date'] = results['baby_deliver_tm']
    print(results)
    return results[['mrn', 'start_date', 'end_date']]

code_pattern = re.compile(r"Z3A(\d+)")
def parse_zcode(diag):
    if diag is None:
        return None
    m = code_pattern.match(diag)
    if m:
        return int(m.group(1))
    return None

def get_pregnancy_records(db):
    columns = ", ".join(["d.enc_num", "d.diag_cd_10", "d.rec_create_dt", "e.mrn", "e.adm_dt", "e.admit_diag_10",
                         "e.princ_diag"])
    table = "DIAGNOSIS_10 d"
    join = "encounter e ON d.enc_num  = e.enc_num"
    where = """                    
      d.diag_cd_10 like 'Z3A%%'  OR
      e.admit_diag_10 like 'Z3A%%'  OR
      e.princ_diag like 'Z3A%%'
     """
    query = db.order_select_query(limit_clause="",
                                  columns=columns,
                                  table=table,
                                  join=join,
                                  where=where)
    results = db.do_select(query)
    results['weeks'] = results['diag_cd_10'].apply(parse_zcode)
    for col in ('admit_diag_10', 'princ_diag'):
        if results[col].any():
            results['weeks'] = results['weeks'].combine_first(results[col].apply(parse_zcode))
    over_42_filter = results['weeks'] > 42
    results.loc[over_42_filter, 'weeks'] = 42
    results['abortion'] = False
    for col in ('diag_cd_10', 'admit_diag_10', 'princ_diag'):
        results['abortion'] = results['abortion'] | (results[col] == 'Z332')
    results['start_date'] = results['adm_dt']  + pd.Timedelta(days=-7) * results['weeks']
    results['end_date'] = results['adm_dt']
    results['end_date'] = results['end_date'].where(results['abortion'],
                                                    results['start_date'] + pd.Timedelta(days=(40*7)))
    print(results)
    return results[['mrn', 'start_date', 'end_date']]
    
