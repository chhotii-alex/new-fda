

def get_zipcodes(db):
    # condor
    for table, datecol in [
            ("REGISTRATION_REF", "rec_create_dt"),
            ("encounter", "adm_dt"),
            I"CLAIM", "claim_dt"),
        ]:
        columns = ", ".join("mrn", "zipcode", datecol)
        table_clause = db.get_prefix() + table
        query = db.order_select_query(
            "",
            columns,
            table_clause,
            "",
            "")
        df = db.do_select(query)
        df['zipcode'] = df['zipcode'].str[:5]
        df['when'] = df[datecol]
        



