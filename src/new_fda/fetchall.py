import re
import pandas as pd
import numpy as np
from sqlalchemy.sql import text
from .util import drop_rows_with_mask
from .querydef import get_queries
from .sourcedb import get_database
from .parse_results import parse_numeric_from_free_u, parse_numeric_from_free_nu, parse_units
from .annotations import get_annotation_queries
from collections import defaultdict
from .parse_hepC_sendout import parse_sendout
from .args import configure_parser
from .ai_classify import classify_result
from tqdm import tqdm
from .demographics import get_demographics, get_demographics2
from .pregnancy import get_delivery_records, get_pregnancy_records
from .comorbid import get_codes, get_diagnoses
from .ses import get_zipcodes
from .census import CensusQuerier
from .immunosuppress import get_meds

def get_mrn_dates(destinationdb, key_columns=['mrn', 'dx_date']):
    def get_all_results(table):
        q = destinationdb.order_select_query(limit_clause=None,
                                         columns=", ".join(key_columns),
                                         table=table, join=None,
                                         where=None, group=None,
                                         order=None, distinct=True)
        return destinationdb.do_select(q)
    results = pd.concat([get_all_results(t) for t in ['results', 'quantresults']])
    return results

def save_interesting(df, destinationdb, key_columns=['mrn', 'dx_date']):
    results = get_mrn_dates(destinationdb, key_columns=key_columns)
    m = pd.merge(results, df, on='mrn', how='left')
    m.dropna(inplace=True)
    if 'dx_date' in key_columns:
        m['diff'] = (m['dx_date'] - m['result_dt']).dt.days
        drop_rows_with_mask(m, (m['diff'] < -30))
        m['diff'] = m['diff'].abs()
        m.sort_values(by='diff', inplace=True)
        m.drop(columns=['diff', 'result_dt'], inplace=True)
    m.drop_duplicates(subset=key_columns, inplace=True)
    novel_columns = [col for col in m.columns if col not in key_columns]
    table_name = "%s" % novel_columns[0].lower()
    destinationdb.do_inserts(table_name, m, key_columns, True)

def do_annotation_queries(virginia, destinationdb):
    data_by_type = defaultdict(list)
    for query, parser in get_annotation_queries():
        result_name = query.name
        q = query.make_query(virginia)  
        print(q)
        df = virginia.do_select(q)
        df.dropna(inplace=True)
        print("Parsing %s results..." % result_name)
        df[result_name] = df['result_value'].progress_apply(parser)
        df.dropna(inplace=True)
        df.drop(columns='result_value', inplace=True)
        data_by_type[result_name].append(df)
    for key in data_by_type.keys():
        df = pd.concat(data_by_type[key])
        df.drop_duplicates(subset=['mrn', 'result_dt'], inplace=True)
        data_by_type[key] = df
    
    # Try to deduce who's a smoker from pack-years records:
    pack_years = data_by_type['pack_years'].sort_values(by=['mrn', 'result_dt'], ignore_index=True)
    prev_pack_years = pack_years.shift(1)
    m = pack_years.join(prev_pack_years, rsuffix='_prev')
    m['diff'] = (m['result_dt'] - m['result_dt_prev']).dt.days
    m['smoking'] = 'current'
    maybe_quit_filter = (m['mrn'] == m['mrn_prev']) & (m['pack_years'] <= m['pack_years_prev']) & (m['diff'] >= 365)
    m.loc[maybe_quit_filter, 'smoking'] = 'former'
    m = m[['mrn', 'result_dt', 'smoking']]
    smoking = pd.concat([data_by_type['smoking'], m])
    smoking.drop_duplicates(subset=['mrn', 'result_dt'], inplace=True)

    # Try to deduce some BMI's from height/weight:
    calc_bmi = pd.merge(data_by_type['Weight'], data_by_type['Height'], how='inner', on=['mrn', 'result_dt'])
    calc_bmi['BMI'] =  calc_bmi['Weight'] * 703 / (calc_bmi['Height'] * calc_bmi['Height'])
    calc_bmi.drop(columns=['Weight', 'Height'], inplace=True)
    bmi = pd.concat([data_by_type['BMI'], calc_bmi])
    bmi.rename(columns={"BMI" : "bmi"}, inplace=True)
    drop_rows_with_mask(bmi, bmi['bmi'] < 10)
    drop_rows_with_mask(bmi, bmi['bmi'] > 100)
    bmi.drop_duplicates(subset=['mrn', 'result_dt'], inplace=True)

    for df in [bmi, smoking]:
        save_interesting(df, destinationdb)


def do_queries_quant(virginia, destinationdb):
    queries = get_queries()
    for query in queries:
        if not query.quantitative: 
            continue
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)
        if df.shape[0] < 1:
            print("Warning, no resultss for this query!")
        age_at_dx(df)
        df['dx'] = query.dx
        df.drop_duplicates(subset=["mrn", "dx_date", "dx"], inplace=True)

        if query.where_clause_value == 'Hepatitis C viral RNA, Quantitative, Real Time PCR':
            df['result_value_num'] = df['comments'].apply(parse_sendout)
            df['units'] = 'UI/mL'
        if 'result_value_num' not in df:
            df['result_value_num'] = np.nan
        def split_df(df):
            has_numeric = df[df['result_value_num'].notna()].copy()
            has_no_numeric = df[df['result_value_num'].isna()].copy()
            return has_numeric, has_no_numeric
        def unite_df(has_numeric, has_no_numeric):
            valid_subsets = [subset for subset in [has_numeric, has_no_numeric] if (subset.shape[0] > 0)]
            return pd.concat(valid_subsets)
        has_numeric, has_no_numeric = split_df(df)
        # The text_result column is never actually used for any of our "quantitative" of interest
        # For those records that have comments, try to parse out a number
        has_no_numeric.dropna(subset="comments", inplace=True)
        print("Parsing numeric results out of comment texts...")
        if 'units' in df:
            has_no_numeric['result_value_num'] = has_no_numeric['comments'].progress_apply(parse_numeric_from_free_nu)
        else:
            has_no_numeric['result_value_num'] = has_no_numeric['comments'].progress_apply(parse_numeric_from_free_u)
            has_no_numeric['units'] = has_no_numeric['comments'].progress_apply(parse_units)
        df = unite_df(has_numeric, has_no_numeric)
        # For those records that still don't have a number,
        # Parse the non-numeric comments to distinguish "not detected" vs. "not done"
        has_numeric, has_no_numeric = split_df(df)
        lookup = {}
        print("Parsing non-numeric commentary...")
        for s in tqdm(has_no_numeric['comments'].unique()):
            lookup[s] = classify_result(s)
        comment_meanings = has_no_numeric['comments'].apply(lambda s: lookup[s])
        zero_or_null = comment_meanings.apply(lambda s: 0 if (s == 'negative') else None)
        has_no_numeric['result_value_num'] = zero_or_null
        df = unite_df(has_numeric, has_no_numeric)
        # Save items for which we found a number
        df.dropna(subset='result_value_num', inplace=True)
        result_cols = query.get_result_columns()
        columns_to_drop = [col for col in result_cols if col not in ['units', 'result_value_num']]
        df.drop(columns=columns_to_drop, inplace=True)
        dest_table_name = 'quantresults'

        destinationdb.do_inserts(dest_table_name, df, ["mrn", "dx_date", "dx"], True)

def age_at_dx(df):
    if df.shape[0] < 1:
        return
    # figure out age at time (clip age at 80 because PHI)
    df.dropna(subset=['dx_date', 'dob'], inplace=True)
    df['age'] = ((df['dx_date'] - df['dob']).dt.days)/365.25
    df['age'] = df['age'].astype(int)
    df.loc[(df['age'] > 80), 'age'] = 80

def do_queries(virginia, destinationdb):
    queries = get_queries()
    for query in queries:
        if query.quantitative: 
            continue
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)
        if df.shape[0] < 1:
            print("Warning, no results for this query!")

        age_at_dx(df)
        df['dx'] = query.dx
        df.drop_duplicates(subset=["mrn", "dx_date", "dx"], inplace=True)

        result_cols = query.get_result_columns()
        if query.table_name == 'vwMICRO_Organisms':
            df['result'] = 'positive'
        else:
            for col in result_cols:
                df[col] = df[col].fillna('')
            df['all_result'] = df[result_cols[0]]
            for i in range(1, len(result_cols)):
                df['all_result'] = df['all_result'] + ' ' + df[result_cols[i]]

            # This is really bad separation of concerns to put this here, but delete all non-WesternBlot Lyme results
            burgdorf = df['all_result'].str.contains('BURGDORFERI') & ~df['all_result'].str.contains('WESTERN')
            drop_rows_with_mask(df, burgdorf)

            print("Number of rows y'all trying to do:")
            print(df.shape[0])
            lookup = {}
            for s in tqdm(df['all_result'].unique()):
                lookup[s] = classify_result(s)
            df['result'] = df['all_result'].apply(lambda s: lookup[s])

            df.drop(columns='all_result', inplace=True)
        df.drop(columns=result_cols, inplace=True)
        dest_table_name = 'results'
        destinationdb.do_inserts(dest_table_name, df, ["mrn", "dx_date", "dx"], True)

def do_queries_chris(virginia, destinationdb):
    queries = get_queries()
    all_df = None
    for query in queries:
        if query.quantitative: # TODO deal with these!!!
            continue
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)

        # figure out age at time (clip age at 80 because PHI)
        df.dropna(subset=['dx_date', 'dob'], inplace=True)
        df['age'] = ((df['dx_date'] - df['dob']).dt.days)/365.25
        df['age'] = df['age'].astype(int)
        df.loc[(df['age'] > 80), 'age'] = 80

        df['dx'] = query.dx
        df.drop_duplicates(subset=["mrn", "dx_date", "dx"], inplace=True)

        result_cols = query.get_result_columns()
        if query.quantitative:
            continue
        else:
            if query.table_name == 'vwMICRO_Organisms':
                df['result'] = 'positive'
                continue
            else:
                for col in result_cols:
                    df[col] = df[col].fillna('')
                df['all_result'] = df[result_cols[0]]
                for i in range(1, len(result_cols)):
                    df['all_result'] = df['all_result'] + ' ' + df[result_cols[i]]

                lookup = {}
                for s in tqdm(df['all_result'].unique()):
                    lookup[s] = classify_result(s)
                df['result'] = df['all_result'].apply(lambda s: lookup[s])

                drop_rows_with_mask(df, df['result'] != 'unknown')
                df = df[['result', 'all_result']]
            df.drop_duplicates(inplace=True)
            if all_df is None:
                all_df = df
            else:
                all_df = pd.concat((all_df, df))
            dest_table_name = 'results'
        print(all_df)
        #destinationdb.do_inserts(dest_table_name, df, ["mrn", "dx_date", "dx"], True)
    all_df.sort_values(by=['result', 'all_result'], inplace=True)
    all_df.to_csv('new_pos_neg_classification.csv', index=False)


def do_distinct_result_queries(virginia):
    queries = get_queries()
    already_seen = set()
    originals = []
    for query in queries:
        if not query.quantitative:
            continue
        if query.table_name == 'vwMICRO_Organisms':
            continue
        q = query.make_count_query(virginia)
        df = virginia.do_select(q)
        total_count = df['row_count'].sum()
        enough = 0.9*total_count
        df['cumsum'] = df['row_count'].cumsum()
        stop_index = df[df['cumsum'] > enough].index[0]
        if (stop_index < 3):
            stop_index = 5
        result_cols = query.get_result_columns()
        for col in result_cols:
            df[col] = df[col].fillna('')
        df['all_result'] = df[result_cols[0]]
        for i in range(1, len(result_cols)):
            df['all_result'] = df['all_result'] + ' ' + df[result_cols[i]]
        print(q)
        print(enough)
        print(stop_index)
        print(df)
        print(df.iloc[:(stop_index+1)])
        for s in df.iloc[:(stop_index+1)]['all_result']:
            s = s.upper()
            if s in already_seen:
                continue
            already_seen.add(s)
            originals.append(s)
        print("...")
    print()
    print("Total distinct string count:")
    print(len(originals))
    with open("quantresults.txt", "w") as f:
        for s in originals:
            f.write(s)
            f.write('\n\n')

def annotate_daterange_state(destinationdb, state_name, data):
    results = get_mrn_dates(destinationdb)
    m = pd.merge(results, data, on='mrn', how='inner')
    filter = (m['start_date'] <= m['dx_date']) & (m['end_date'] >= m['dx_date'])
    m = m[filter].copy()
    m[state_name] = True
    m.drop(columns=['start_date', 'end_date'], inplace=True)
    m.drop_duplicates(inplace=True)
    destinationdb.do_inserts(state_name, m[['mrn', 'dx_date', state_name]], ['mrn', 'dx_date'])

def annotate_pregnancy(virginia, condor, destinationdb):
    pregs = pd.concat( (get_pregnancy_records(condor),
                        get_delivery_records(virginia)) )
    annotate_daterange_state(destinationdb, 'pregnancy', pregs)
    
def annotate_immunosuppression(virginia, destinationdb):
    meds = get_meds(virginia)
    annotate_daterange_state(destinationdb, 'immunosuppressed', meds)

def do_demographics(virginia, condor, destinationdb):
    df = pd.concat((get_demographics(virginia),
                    get_demographics2(condor)))
    df.drop_duplicates(inplace=True)
    save_interesting(df, destinationdb,
                         key_columns=['mrn'])

def do_comorbidities(condor, destinationdb):
    all_codes = get_codes(destinationdb)
    for tranche in range(10):
        df = get_diagnoses(condor, all_codes, tranche)
        results = get_mrn_dates(destinationdb)
        m = pd.merge(results, df, on='mrn', how='inner')
        m['diff'] = (m['dx_date'] - m['adm_dt']).dt.days
        drop_rows_with_mask(m, m['diff'] < 30)
        m.drop(columns=['diff', 'adm_dt'], inplace=True)
        m.drop_duplicates(inplace=True)
        destinationdb.do_inserts('comorbidity', m, ['mrn', 'dx_date'])

def do_ses(condor, destinationdb):
    q = CensusQuerier()
    for tranche in range(10):
        ses = get_zipcodes(condor, tranche)
        results = get_mrn_dates(destinationdb,
                                key_columns=['mrn', 'dx_date'])
        m = pd.merge(results, ses, on='mrn', how='left')
        m.dropna(inplace=True)
        m['diff'] = (m['dx_date'] - m['result_dt']).dt.days
        drop_rows_with_mask(m, (m['diff'] < -356))
        m['diff'] = m['diff'].abs()
        m.sort_values(by='diff', inplace=True)
        m.drop(columns=['diff', 'result_dt'], inplace=True)
        m.drop_duplicates(subset=['mrn', 'dx_date'], inplace=True)
        m['ses'] = m['zipcode'].progress_apply(q.ses_bin_for_zip)
        q.cache_data()
        m.drop(columns=['zipcode'], inplace=True)
        destinationdb.do_inserts("ses", m, ['mrn', 'dx_date'], True)
    
def get_tags(destinationdb):
    q = 'SELECT "short_name" FROM "comorbidity_lookup"'
    tags = destinationdb.do_select(q)
    return list(tags['short_name'])
        
def merge_data(destinationdb):
    tags = get_tags(destinationdb)
    for rtable in ['quantresults', 'results']:
        new_table_name = '%s_public' % rtable
        drop_statement = 'DROP table ' + new_table_name
        with destinationdb.engine.connect() as con:
            con.execute(text(drop_statement))
            con.commit()
        breakpoint()
        for tranche in range(10):
            where = " WHERE mrn like '%s%d' " % ('%%', tranche)
            q = "SELECT * from " + rtable + where
            print(q)
            df = destinationdb.do_select(q)
            df.drop(columns='dob', inplace=True)
            for tag in tags:
                q = "SELECT mrn, dx_date, 1 %s from comorbidity %s and tag = '%s'" % (tag, where, tag)
                comorbids = destinationdb.do_select(q)
                df = df.merge(comorbids, on=['mrn', 'dx_date'], how="left")
                tag = tag.lower()
                df[tag] = df[tag].fillna(0)
                print(tag, df[tag].sum())
            for table in [
                "bmi",
                "ses",
                "smoking",
            ]:
                q = "SELECT * from " + table + where
                anno = destinationdb.do_select(q)
                df = df.merge(anno, on=["mrn", "dx_date"], how="left")
            for table in [
                "immunosuppressed",
                "pregnancy",
            ]:
                q = "SELECT * from " + table + where
                anno = destinationdb.do_select(q)
                df = df.merge(anno, on=["mrn", "dx_date"], how="left")
                df[table] = df[table].fillna(0)
            for table in [
                "race"
            ]:
                q = "SELECT * from " + table + where
                anno = destinationdb.do_select(q)
                df = df.merge(anno, on="mrn", how="left")
            df.to_sql(new_table_name, destinationdb.engine, if_exists='append', index=False, method='multi')

def main():
    tqdm.pandas()

    arg = configure_parser()
    virginia = get_database('virginia')
    condor = get_database('condor')
    destinationdb = get_database('newfda')
    if arg.redo:
        destinationdb.build_schema()
        print("Built db schema")

    if arg.redo or arg.step < 1:
        do_queries(virginia, destinationdb)
    if arg.redo or arg.step < 2:
        do_queries_quant(virginia, destinationdb)
    if arg.redo or arg.step < 3:
        do_comorbidities(condor, destinationdb)
    if arg.redo or arg.step < 4:
        do_ses(condor, destinationdb)
    if arg.redo or arg.step < 5:
        do_demographics(virginia, condor, destinationdb)
    if arg.redo or arg.step < 6:
        do_annotation_queries(virginia, destinationdb)
    if arg.redo or arg.step < 7:
        annotate_pregnancy(virginia, condor, destinationdb)
    if arg.redo or arg.step < 8:
        annotate_immunosuppression(virginia, destinationdb)

    merge_data(destinationdb)
