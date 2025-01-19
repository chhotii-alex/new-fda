import pandas as pd
import numpy as np
from .querydef import get_queries
from .sourcedb import get_database
from .parse_results import extract_result
from .annotations import get_annotation_queries

def do_annotation_queries(virginia, destinationdb):
    for query, parser in get_annotation_queries():
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)
        df.dropna(inplace=True)
        df['extracted'] = df['result_value'].apply(parser)
        df.dropna(inplace=True)
        df.drop(columns='result_value', inplace=True)
        print(df)

def do_queries(virginia, destinationdb):
    queries = get_queries()
    for query in queries:
        q = query.make_query(virginia)
        print(q)
        df = virginia.do_select(q)

        # figure out age at time (clip age at 80 because PHI)
        df.dropna(subset=['dx_date', 'dob'], inplace=True)
        df['age'] = ((df['dx_date'] - df['dob']).dt.days)/365.25
        df['age'] = df['age'].astype(int)
        df.loc[(df['age'] > 80), 'age'] = 80

        result_cols = query.get_result_columns()
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

        df['dx'] = query.dx
        df.drop_duplicates(subset=["mrn", "dx_date", "dx"], inplace=True)
        destinationdb.do_inserts('secret.results', df, ["mrn", "dx_date", "dx"], True)


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

    do_annotation_queries(virginia, destinationdb)
    #do_distinct_result_queries(virginia)
    #do_queries(virginia, destinationdb)
