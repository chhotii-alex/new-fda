from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from .querydef import get_queries
from .stringsubs import do_substitutions

def do_queries(virginia):
    queries = get_queries()
    for query in queries:
        q = query.make_query(100)
        df = pd.read_sql(q, virginia)
        df['age'] = ((df[query.get_date_col()] - df['dob']).dt.days)/365.25
        df['age'] = df['age'].astype(int)
        df.loc[(df['age'] > 80), 'age'] = 80
        print(query.where_clause_value)
        print(df.head(50))

def do_distinct_result_queries(virginia):
    queries = get_queries()
    already_seen = set()
    originals = []
    for query in queries:
        if query.quantitative:
            continue
        if query.table_name == 'vwMICRO_Organisms':
            continue
        q = query.make_count_query()
        df = pd.read_sql(q, virginia)
        """
        if type(df[query.get_date_col()].dtype) != np.dtypes.DateTime64DType:
            raise Exception("Incorrect type for date column")
        """
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
    with open("results.txt", "w") as f:
        for s in originals:
            f.write(s)
            f.write('\n\n')

def main():
    virginia = create_engine("mssql+pyodbc://virginia")

    do_queries(virginia)
