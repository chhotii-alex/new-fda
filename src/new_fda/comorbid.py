import pandas as pd
from importlib import resources
from . import comorbidities

hcups_descriptions = pd.read_csv(resources.files(comorbidities) / "CMR-Reference-File_desc.csv")
hcups_codes = pd.read_csv(resources.files(comorbidities) / "CMR-Reference-File_codes.csv")
hcups_descriptions["short_name"] = hcups_descriptions["Abbreviation (SAS Data Element Name)"].str.slice(start=4)
hcups_descriptions['tag_len'] = hcups_descriptions['short_name'].str.len()
print(hcups_descriptions)
print(hcups_descriptions['tag_len'].max())

all_codes = None
for tag in hcups_descriptions["short_name"]:
    code_filter = hcups_codes[tag].astype('boolean')
    codes_for_this_tag = pd.DataFrame({
        'icd10' : hcups_codes.loc[code_filter, "ICD-10-CM Diagnosis"].copy(),
        'tag' : tag
    })
    if all_codes is None:
        all_codes = codes_for_this_tag
    else:
        all_codes = pd.concat([all_codes, codes_for_this_tag])

def get_admit_diagnoses(db, where, limit_clause):
    table = db.get_prefix() + "encounter e"
    columns = "e.mrn,  e.admit_diag_10 icd10, e.adm_dt"
    query = db.order_select_query(limit_clause, columns, table, None, where)
    print(query)
    return db.do_select(query)

def get_addl_diagnoses(db, where, limit_clause):
    table = db.get_prefix() + "DIAGNOSIS_10 d"
    join = "%sencounter e ON d.enc_num = e.enc_num" % db.get_prefix()
    columns = "e.mrn,  d.diag_cd_10 icd10, e.adm_dt"
    query = db.order_select_query(limit_clause, columns, table, join, where)
    print(query)
    return db.do_select(query)

def get_diagnoses(db, tranche=None, limit=None):
    if limit:
        limit_clause = db.make_limit_clause(limit)
    else:
        limit_clause = ""
    if tranche is not None:
        where = 'mrn like \'%s%d\' ' % ('%%', tranche)
    df = pd.concat([get_admit_diagnoses(db, where, limit_clause),
                    get_addl_diagnoses(db, where, limit_clause)])
    df['icd10'] = df['icd10'].str.strip()
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    m = df.merge(all_codes, on='icd10', how='inner')
    m.drop_duplicates(subset=['mrn', 'adm_dt', 'tag'], inplace=True)
    return m[['mrn', 'adm_dt', 'tag']]
