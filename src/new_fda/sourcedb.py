import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from .cache import ResultsCacher

class Database:
    def __init__(self, schema):
        self.schema = schema
        self.engine = create_engine(self.get_connection_string())
        self.cache = None

    def allow_caching(self):
        return True
    
    def get_name(self):
        return self.schema.get_name()
    
    def do_select(self, query_text):
        if self.allow_caching():
            if self.cache is None:
                self.cache = self.make_cache()
            if self.cache.has_results(query_text):
                return self.cache.results(query_text)
        results = pd.read_sql(query_text, self.engine)
        if self.allow_caching():
            self.cache.save_results(query_text, results)
        return results
    
    def make_cache(self):
        return ResultsCacher(self)

class PostgresDatabase(Database):
    def get_connection_string(self):
        return "postgresql+psycopg2://postgres:%s@localhost:5434/%s" % ("thisisfake", self.schema.get_name())

    def get_prefix(self):
        return ""

    def make_limit_clause(self, n):
        if n is None:
            return " "
        else:
            return f" LIMIT {n} "

    def order_select_query(self, limit_clause, columns, table, join, where):
        return f"""SELECT
             {columns}
           FROM {table}
           JOIN {join}
           WHERE {where}
           {limit_clause}
        """
    
    def allow_caching(self):
        return False

class SQLServerDatabase(Database):
    def get_connection_string(self):
        return "mssql+pyodbc://%s" % self.schema.get_name()

    def get_prefix(self):
        return self.schema.get_prefix()

    def make_limit_clause(self, n):
        if n is None:
            return " "
        else:
            return f" TOP {n} "

    def order_select_query(self, limit_clause, columns, table, join, where):
        return f"""SELECT
           {limit_clause}
             {columns}
           FROM {table}
           JOIN {join}
           WHERE {where}
        """
class Schema:
    pass

class Virginia(Schema):
    def get_prefix(self):
        return "[MDW_Analytics].[dbo]."

    def get_name(self):
        return "virginia"

class Condor(Schema):
    def get_prefix(self):
        return "[Casemix_TSI].[dbo]."

    def get_name(self):
        return "condor"
    
def get_database(name):
    if name == "virginia":
        schema = Virginia()
    elif name == "condor":
        schema = Condor()
    else:
        raise Exception("Unknown source database specified")
    if os.name == "nt":
        db = SQLServerDatabase(schema)
    else:
        print("Warning, running in test mode", file=sys.stderr)
        db = PostgresDatabase(schema)
    return db
