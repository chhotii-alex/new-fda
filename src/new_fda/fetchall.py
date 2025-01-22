import re
import pandas as pd
import numpy as np
from .util import drop_rows_with_mask
from .querydef import get_queries
from .sourcedb import get_database
from .parse_results import parse_numeric_from_free
from .annotations import get_annotation_queries
from collections import defaultdict
from .parse_hepC_sendout import parse_sendout
from .args import configure_parser
from .ai_classify import classify_result
from tqdm import tqdm

def save_interesting(df, destinationdb):
    table_name = "secret.%s" % df.columns[2].lower()
    q = destinationdb.order_select_query(limit_clause=None, columns='mrn, dx_date', table='secret.results', join=None, where=None, group=None, order=None, distinct=True)
    results = destinationdb.do_select(q)
    m = pd.merge(results, df, on='mrn', how='left')
    m.dropna(inplace=True)
    m['diff'] = (m['dx_date'] - m['result_dt']).dt.days
    drop_rows_with_mask(m, (m['diff'] < -30))
    m['diff'] = m['diff'].abs()
    m.sort_values(by='diff', inplace=True)
    m.drop_duplicates(subset=['mrn', 'dx_date'], inplace=True)
    m.drop(columns=['diff', 'result_dt'], inplace=True)
    destinationdb.do_inserts(table_name, m, list(m.columns[:2]), True)

def do_annotation_queries(virginia, destinationdb):
    data_by_type = defaultdict(list)
    for query, parser in get_annotation_queries():
        result_name = query.name
        q = query.make_query(virginia, 9000)  # TODO remove the limit
        print(q)
        df = virginia.do_select(q)
        df.dropna(inplace=True)
        df[result_name] = df['result_value'].apply(parser)
        df.dropna(inplace=True)
        df.drop(columns='result_value', inplace=True)
        data_by_type[result_name].append(df)
        print(df)
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
        if 'result_value_num' not in df:
            df['result_value_num'] = np.nan
        has_numeric = df[df['result_value_num'].notna()].copy()
        has_no_numeric = df[df['result_value_num'].isna()].copy()
        has_no_numeric.dropna(subset="comments", inplace=True)
        has_no_numeric['result_value_num'] = has_no_numeric['comments'].apply(parse_numeric_from_free)
        for subset in [has_numeric, has_no_numeric]:
            subset.dropna(subset='result_value_num', inplace=True)
        valid_subsets = [subset for subset in [has_numeric, has_no_numeric] if (subset.shape[0] > 0)]
        if not len(valid_subsets):
            print("No numeric results found!!!")
            continue
        df = pd.concat(valid_subsets)
        print(df)
        result_cols = query.get_result_columns()
        columns_to_drop = [col for col in result_cols if col != 'result_value_num']
        df.drop(columns=columns_to_drop, inplace=True)
        dest_table_name = 'secret.quantresults'

        print(df.head())
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
        dest_table_name = 'secret.results'
        print(df)
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
            dest_table_name = 'secret.results'
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

def main():
    arg = configure_parser()
    virginia = get_database('virginia')
    destinationdb = get_database('newfda')
    if arg.redo:
        destinationdb.build_schema()
        print("Built db schema")

    do_queries(virginia, destinationdb)
    do_queries_quant(virginia, destinationdb)
    do_annotation_queries(virginia, destinationdb)
    #do_distinct_result_queries(virginia)
