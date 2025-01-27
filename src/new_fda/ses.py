import pandas as pd

def get_zipcodes(db, tranche=None):
    # condor
    all_zips = None
    for table, datecol in [
            ("REGISTRATION_REF", "rec_create_dt"),
            ("encounter", "adm_dt"),
            ("CLAIM", "claim_dt"),
        ]:
        columns = ", ".join(["mrn", "zipcode", datecol])
        table_clause = db.get_prefix() + table
        if tranche is not None:
            where = "mrn like '%s%d'" % ('%%', tranche)
        else:
            where = None
        query = db.order_select_query(
            "",
            columns,
            table_clause,
            None,
            where)
        print(query)
        df = db.do_select(query)
        df['zipcode'] = df['zipcode'].str[:5]
        df.dropna(inplace=True)
        if df.shape[0] < 1:
            continue
        df['result_dt'] = df[datecol]

        df = df[['mrn', 'result_dt', 'zipcode']]
        if all_zips is None:
            all_zips = df
        elif df is not None:
            all_zips = pd.concat([all_zips, df])
            
    all_zips.dropna(inplace=True)
    all_zips.sort_values(by=['mrn', 'result_dt'])
    all_zips.drop_duplicates(subset=['mrn', 'zipcode'], keep='first')
    return all_zips

