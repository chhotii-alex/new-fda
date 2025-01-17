import os
import sys
from sqlalchemy import create_engine

class Database:
    def __init__(self, schema):
        self.schema = schema
        self.engine = create_engine(self.get_connection_string())

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
