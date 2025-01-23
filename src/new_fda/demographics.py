import pandas as pd

# The race/ethnicity/national origin data in the registration table
# is a complete mess.
# WH - white
# BL - blck
# AS - asian or pacific islander
# HS - hispanic
# NA - native american
codes = {"1": "WH", "2": "BL", "3": "AS", "4": "HS", "5": "NA", "9": "AS"}

def code_ethnicity(ethnic_cd, hispanic_ind, race, race_full, ethnicity):
    if race in ("WH", "BL", "AS"):
        return race
    if race_full is not None:
        if "AMERICAN INDIAN" in race_full:
            return "NA"
        elif "ASIAN" in race_full:
            return "AS"
        elif "BLACK" in race_full:
            return "BL"
        elif "HISPANIC" in race_full:
            return "HS"
        elif "HAWAIIAN" in race_full:
            return "AS"
        elif "WHITE" in race_full:
            return "WH"
    if ethnic_cd is not None:
        if ethnic_cd[0] in ("1", "2", "3", "4", "5", "9"):
            return codes[ethnic_cd[0]]
    if hispanic_ind == "Y":
        return "HS"
    if ethnicity is not None:
        ethnicity = ethnicity.strip()
        if ethnicity in ("UGANDAN", "CAPE VERDEA", "AFRICAN", "AFRICAN AME", "HAITIAN"):
            return "BL"
        if ethnicity in (
            "ASIAN",
            "ASIAN INDIA",
            "CHINESE",
            "VIETNAMESE",
            "KOREAN",
            "FILIPINO",
            "PAKISTANI",
        ):
            return "AS"
        if ethnicity in ("PORTUGUESE", "EUROPEAN", "RUSSIAN", "EASTERN EUR", "ITALIAN"):
            return "WH"
    return None

def get_demographics(db, limit=None):
    limit_clause = db.make_limit_clause(limit)
    columns = ", ".join(['mrn', 'race', 'race_full', 'race_desc'])
    q = db.order_select_query(limit_clause, columns, 'vwADT_Admissions',
                              join=None, where=None,
                              group=None, order=None, distinct=False)
    print(q)
    df = db.do_select(q)
    print(df)
    print("Apply>..")
    f = lambda row: code_ethnicity(None, None, row.race, row.race_full, row.race_desc)
    df['race'] = df.apply(f, 1)
    df.dropna(subset='race', inplace=True, ignore_index=True)
    df.drop_duplicates(subset='mrn', inplace=True, ignore_index=True)
    df.drop(columns=['race_full', 'race_desc'], inplace=True)
    return df

