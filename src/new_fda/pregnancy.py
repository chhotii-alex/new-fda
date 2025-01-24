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
    table = db.get_prefix() + 'vwADT_Admissions'
    join = None
    where = 'baby_deliver_tm is not null'

    limit_clause = ''   #db.make_limit_clause(2000) # TODO remove
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
    breakpoint()
    columns = ", ".join(["d.enc_num", "d.diag_cd_10", "d.rec_create_dt", "e.mrn", "e.adm_dt", "e.admit_diag_10",
                         "e.princ_diag"])
    table = db.get_prefix() + "DIAGNOSIS_10 d"
    join = "%sencounter e ON d.enc_num  = e.enc_num" % db.get_prefix()
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
    weeks_from_code = None
    for col in ['diag_cd_10', 'princ_diag', 'admit_diag_10']:
        weeks_from_col = results[col].apply(parse_zcode)
        if weeks_from_col.notna().sum():
            if weeks_from_code is None:
                weeks_from_code = weeks_from_col
            else:
                weeks_from_code = weeks_from_code.combine_first(weeks_from_col)
    results['weeks'] = weeks_from_code
    over_42_filter = results['weeks'] > 42
    results.loc[over_42_filter, 'weeks'] = 42
    results['abortion'] = False
    for col in ('diag_cd_10', 'admit_diag_10', 'princ_diag'):
        results['abortion'] = results['abortion'] | (results[col] == 'Z332')
    results['start_date'] = results['adm_dt']  + pd.Timedelta(days=-7) * results['weeks']
    results['end_date'] = results['adm_dt']
    results['end_date'] = results['end_date'].where(results['abortion'],
                                                    results['start_date'] + pd.Timedelta(days=(40*7)))
    return results[['mrn', 'start_date', 'end_date']]
    
