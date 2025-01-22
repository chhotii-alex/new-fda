import re
import pandas as pd
import numpy as np
from .util import drop_rows_with_mask
from .querydef import get_queries
from .sourcedb import get_database
from .parse_results import extract_result, parse_numeric_from_free
from .annotations import get_annotation_queries
from collections import defaultdict

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
    all_results = None
    for query in queries:
        if not query.quantitative: 
            continue
        if query.where_clause_value == 'Hepatitis C viral RNA, Quantitative, Real Time PCR':
            print(query.make_query(virginia))
            sys.exit(1)
            continue
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)

        df['dx'] = query.dx

        if 'result_value_num' in df:
            drop_rows_with_mask(df, df['result_value_num'].notna())
        df.drop_duplicates(subset='comments', inplace=True)

        print(df)
        if all_results is None:
            all_results = df['comments']
        else:
            all_results = pd.concat((all_results, df['comments']))
        #destinationdb.do_inserts(dest_table_name, df, ["mrn", "dx_date", "dx"], True)
    #all_results.to_csv("quant_results.csv", index=False)
    all_results.dropna(inplace=True)
    all_results.drop_duplicates(inplace=True)
    all_results.sort_values(inplace=True)
    counter = 0
    for i, comm in enumerate(all_results):
        result = parse_numeric_from_free(comm)
        if result is None:
            print(result)
            print(comm)
            print()
            counter += 1
            #if not (i % 100):
            #    input("?>")
    print(counter, " numeric results")

def do_queries(virginia, destinationdb):
    queries = get_queries()
    for query in queries:
        if not query.quantitative: # DNCI !!! TODO deal with these!!!
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
                df.to_excel("irk.xlsx")
                continue
            df = pd.concat(valid_subsets)
            print(df)
            columns_to_drop = [col for col in result_cols if col != 'result_value_num']
            df.drop(columns=columns_to_drop, inplace=True)
            dest_table_name = 'secret.quantresults'
        else:
            if query.table_name == 'vwMICRO_Organisms':
                df['result'] = 'positive'
            else:
                for col in result_cols:
                    df[col] = df[col].fillna('')
                df['all_result'] = df[result_cols[0]]
                for i in range(1, len(result_cols)):
                    df['all_result'] = df['all_result'] + ' ' + df[result_cols[i]]
                df['result'] = df['all_result'].apply(extract_result)
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
            if query.table_name == 'vwLAB_Result':
                pass
            dest_table_name = 'secret.quantresults'
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
                df['result'] = df['all_result'].apply(extract_result)
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
    virginia = get_database('virginia')
    destinationdb = get_database('newfda')

    #do_queries(virginia, destinationdb)
    do_annotation_queries(virginia, destinationdb)
    #do_distinct_result_queries(virginia)
