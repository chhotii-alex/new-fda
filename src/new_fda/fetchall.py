from sqlalchemy import create_engine
import pandas as pd

def do_queries(dx):
    pass

def get_lab_results(virginia):
    for dx in [
        "RUBELLA",
        'VARICELLA-ZOSTER',
        'CLOSTRIDIUM DIFFICILE',
        'HELIOBACTER PYLORI',
        'RUBEOLA',
        'HIV-1',
        'HCV',
        'BETA STREP GROUP A',
        'HBC',
        'MUMPS',
        'LYME',
        'CYPTOSPORIDIUM',
        'CMV',
    ]:
        do_queries(dx)

def main():
    virginia = create_engine("mssql+pyodbc://virginia")

    get_lab_results(virginia)

    query = """
SELECT top 500 *
FROM  [MDW_Analytics].[dbo].[vwMICRO_Test]
WHERE (test_name = 'RUBELLA IgG SEROLOGY' OR test_name = 'RUBELLA SEROLOGY')
"""

    df = pd.read_sql(query, virginia)
    print(df)
    print("Here is the new command!")
