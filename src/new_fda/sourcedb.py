import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from .cache import ResultsCacher
from .encoding import get_encode_keys
from sqlalchemy.dialects.postgresql import insert

class Database:
    def __init__(self, schema):
        self.schema = schema
        self.engine = create_engine(self.get_connection_string())
        self.cache = None

    def allow_caching(self):
        return True
    
    def get_name(self):
        return self.schema.get_name()
    
    def _do_select(self, query_text):
        if self.allow_caching():
            if self.cache is None:
                self.cache = self.make_cache()
            if self.cache.has_results(query_text):
                return self.cache.results(query_text)
        results = pd.read_sql(query_text, self.engine)
        if self.allow_caching():
            self.cache.save_results(query_text, results)
        return results

    def do_select(self, query_text):
        results = self._do_select(query_text)
        if 'mrn' in results.columns:
            results.dropna(subset='mrn', inplace=True)
            results['mrn'] = pd.to_numeric(results['mrn'], errors='coerce',
                                                       downcast='integer')
            results.dropna(subset='mrn', inplace=True)
            results['offset'] = (results['mrn'] % 1000) - 500
            results['offset'] = pd.to_timedelta(results['offset'], unit="d")
            secret_info = get_encode_keys()
            results['mrn'] = (results['mrn'] * secret_info[0]) % secret_info[1]
            results['mrn'] = 'p' + results['mrn'].astype(str).str.zfill(10)
            for col in results:
                if pd.api.types.is_datetime64_any_dtype(results[col].dtype):
                    results[col] = results[col] + results['offset']
            results.drop(columns='offset', inplace=True)
        return results
    
    def make_cache(self):
        return ResultsCacher(self)
    
class PostgresDatabase(Database):
    def get_connection_string(self):
        s = "postgresql+psycopg2://postgres:%s@localhost:%d/%s" % (self.get_password(), self.get_port(), self.schema.get_name())
        return s

    def get_port(self):
        return 5434
    
    def get_password(self):
        return "thisisfake"

    def get_prefix(self):
        return ""

    def make_limit_clause(self, n):
        if n is None:
            return " "
        else:
            print("WARNING, using a limit!")
            return f" LIMIT {n} "

    def order_select_query(self, limit_clause, columns, table, join=None, where=None, group=None, order=None, distinct=False):
        table = table.lower()
        if limit_clause is None:
            limit_clause = ''
        if 'limit' in limit_clause.lower():
            print("WARNING, using a limit")
        if join is None:
            join_clause = ''
        else:
            join_clause = f'JOIN {join}'
        if group is None:
            group_clause = ""
        else:
            group_clause = f'GROUP BY {group}'
        if order is None:
            order_clause = ''
        else:
            order_clause = f'ORDER BY {order}'
        if distinct:
            distinct_clause = 'distinct'
        else:
            distinct_clause = ''
        if where is None:
            where_clause = ""
        else:
            where_clause = f"WHERE {where}"
        return f"""SELECT {distinct_clause}
             {columns}
           FROM {table}
           {join_clause}
           {where_clause}
            {group_clause}
            {order_clause}
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
            print("WARNING, using a limit")
            return f" TOP {n} "

    def order_select_query(self, limit_clause, columns, table, join=None, where=None, group=None, order=None, distinct=False):
        if where is None:
            where_clause = ''
        else:
            where_clause = f'WHERE {where}'
        if join is None:
            join_clause = ''
        else:
            join_clause = f'JOIN {join}'
        if group is None:
            group_clause = ""
        else:
            group_clause = f'GROUP BY {group}'
        if order is None:
            order_clause = ''
        else:
            order_clause = f'ORDER BY {order}'
        if distinct:
            distinct_clause = 'distinct'
        else:
            distinct_clause = ''
        return f"""SELECT {distinct_clause}
           {limit_clause}
             {columns}
           FROM {table}
           {join_clause}
           {where_clause}
            {group_clause}
            {order_clause}
        """
    
class DestinationDatabase(PostgresDatabase):
    def get_port(self):
        if os.name == 'nt':
            return super().get_port()
        else:
            return 5435


    def build_schema(self):
        with self.engine.connect() as con:
            for statement in [
                """
                    CREATE USER webapp WITH PASSWORD 'LieutenantCommander';
                """,
                """
                    DROP TABLE IF EXISTS "bmi";
                """,
                """
    CREATE TABLE "public"."bmi" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "bmi" double precision
) WITH (oids = false);
                """,
                """
            CREATE INDEX "ix_bmi_dx_date" ON "public"."bmi" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_bmi_mrn" ON "public"."bmi" USING btree ("mrn");

                """,
                """
    ALTER TABLE "public"."bmi"
    ADD CONSTRAINT unique_keys_bmi UNIQUE(mrn, dx_date);
                """,
                """
                    DROP TABLE IF EXISTS "ses";
                """,
                """
    CREATE TABLE "public"."ses" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "ses" smallint
) WITH (oids = false);
                """,
                """
            CREATE INDEX "ix_ses_dx_date" ON "public"."ses" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_ses_mrn" ON "public"."ses" USING btree ("mrn");

                """,
                """
    ALTER TABLE "public"."ses"
    ADD CONSTRAINT unique_keys_ses UNIQUE(mrn, dx_date);
                """,

                """
                    DROP TABLE IF EXISTS "pregnancy";
                """,
                """
    CREATE TABLE "public"."pregnancy" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "pregnancy" boolean
) WITH (oids = false);
                """,
                """
            CREATE INDEX "ix_pregnancy_dx_date" ON "public"."pregnancy" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_pregnancy_mrn" ON "public"."pregnancy" USING btree ("mrn");

                """,
                """
    ALTER TABLE "public"."pregnancy"
    ADD CONSTRAINT unique_keys_pregnancy UNIQUE(mrn, dx_date);
                """,

                """
                    DROP TABLE IF EXISTS "immunosuppressed";
                """,
                """
    CREATE TABLE "public"."immunosuppressed" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "immunosuppressed" boolean
) WITH (oids = false);
                """,
                """
            CREATE INDEX "ix_immunosuppressed_dx_date" ON "public"."immunosuppressed" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_immunosuppressed_mrn" ON "public"."immunosuppressed" USING btree ("mrn");

                """,
                """
    ALTER TABLE "public"."immunosuppressed"
    ADD CONSTRAINT unique_keys_immunosuppressed UNIQUE(mrn, dx_date);
                """,

                """
DROP TABLE IF EXISTS "quantresults";
                """,
                """
CREATE TABLE "public"."quantresults" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "dx" text NOT NULL,
    "gender" character(1),
    "dob" timestamp,
    "pat_type_full" text,
    "age" smallint,
    "result_value_num" double precision,
    "units" text,
    "result_value_log10" double preceison NULL
) WITH (oids = false);

                """,
                """
CREATE INDEX "ix_quantresults_dx" ON "public"."quantresults" USING btree ("dx");
                """,
                """
CREATE INDEX "ix_quantresults_dx_date" ON "public"."quantresults" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_quantresults_mrn" ON "public"."quantresults" USING btree ("mrn");
                """,
                """
    ALTER TABLE "public"."quantresults"
    ADD CONSTRAINT unique_keys_quantresults UNIQUE(mrn, dx_date, dx);
                """,
                """
DROP TABLE IF EXISTS "results";
                """,
                """
CREATE TABLE "public"."results" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "dx" text NOT NULL,
    "gender" character(1),
    "dob" timestamp,
    "pat_type_full" text,
    "age" smallint,
    "result" text
) WITH (oids = false);
                """,
                """
CREATE INDEX "ix_results_dx" ON "public"."results" USING btree ("dx");
                """,
                """
CREATE INDEX "ix_results_dx_date" ON "public"."results" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_results_mrn" ON "public"."results" USING btree ("mrn");
                """,
                """
    ALTER TABLE "public"."results"
    ADD CONSTRAINT unique_keys_results UNIQUE(mrn, dx_date, dx);
                """,
                """
DROP TABLE IF EXISTS "smoking";
                """,
                """
CREATE TABLE "public"."smoking" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "smoking" text
) WITH (oids = false);
                """,
                """
CREATE INDEX "ix_smoking_dx_date" ON "public"."smoking" USING btree ("dx_date");
                """,
                """
CREATE INDEX "ix_smoking_mrn" ON "public"."smoking" USING btree ("mrn");
                """,
                """
    ALTER TABLE "public"."smoking"
    ADD CONSTRAINT unique_keys_smoking UNIQUE(mrn, dx_date);
                """,

                """
DROP TABLE IF EXISTS "comorbidity_lookup";
                """,
                """
CREATE TABLE "public"."comorbidity_lookup" (
    "short_name" text NOT NULL,
    "description" text
) WITH (oids = false);
                """,
                """
CREATE INDEX "ix_comorbidity_lookup_short_name" ON "public"."comorbidity_lookup" USING btree ("short_name");
                """,
                """
    ALTER TABLE "public"."comorbidity_lookup"
    ADD CONSTRAINT unique_keys_comorbidity_lookup UNIQUE(short_name);
                """,
                """grant select on comorbidity_lookup to webapp;""",
                

                """
DROP TABLE IF EXISTS "comorbidity";
                """,
                """
CREATE TABLE "public"."comorbidity" (
    "mrn" character(11) NOT NULL,
    "dx_date" timestamp NOT NULL,
    "tag" text
) WITH (oids = false);
                """,
                """
CREATE INDEX "ix_comorbidity_mrn" ON "public"."comorbidity" USING btree ("mrn");
                """,
                """
CREATE INDEX "ix_comorbidity_dx_date" ON "public"."comorbidity" USING btree ("dx_date");
                """,
                """
    ALTER TABLE "public"."comorbidity"
    ADD CONSTRAINT unique_keys_comorbidity UNIQUE(mrn, dx_date);
                """,


                """
DROP TABLE IF EXISTS "race";
                """,
                """
CREATE TABLE "public"."race" (
    "mrn" character(11) NOT NULL,
    "race" character(2)
) WITH (oids = false);
                """,
                """
CREATE INDEX "ix_race_mrn" ON "public"."race" USING btree ("mrn");
                """,
                """
    ALTER TABLE "public"."race"
    ADD CONSTRAINT unique_keys_race UNIQUE(mrn);
                """,
            ]:
                con.execute(text(statement))
                con.commit()
                print("Did statement:")
                print(statement)
                print()
    
    """
    Add something like this to database creation schema: 
    ALTER TABLE "results"
    ADD CONSTRAINT unique_keys UNIQUE(mrn, dx_date, dx);
    See:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    https://docs.sqlalchemy.org/en/20/dialects/postgresql.html

    """
    def do_inserts(self, table_name, df, index_cols, first_time=False):
        df = df.groupby(index_cols).first()
        def insert_on_conflict_nothing(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=index_cols)
            result = conn.execute(stmt)
            return result.rowcount
        df.to_sql(name=table_name, con=self.engine, if_exists="append", chunksize=128, method=insert_on_conflict_nothing)  
        
    
    def get_password(self):
        if os.name == "nt":
            base_path = Path("C:\\shared\\reagan")
        else:
            base_path = Path("~").expanduser() / "fakedatabases"
        password_file_path = base_path / self.get_name() / "password.txt"
        with open(password_file_path, "r") as f:
            password = f.readline().strip()
        return password

    def do_select(self, query_text):
        return self._do_select(query_text)

class Schema:
    def database_type(self):
        return "source"

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
    
class Destination(Schema):
    def get_name(self):
        return "newfda"

    def database_type(self):
        return "destination"

def get_database(name):
    if name == "virginia":
        schema = Virginia()
    elif name == "condor":
        schema = Condor()
    elif name == "newfda":
        schema = Destination()
    else:
        raise Exception("Unknown source database specified")
    if schema.database_type() == "destination":
        db = DestinationDatabase(schema)
    else:
        if os.name == "nt":
            db = SQLServerDatabase(schema)
        else:
            print("Warning, running in test mode", file=sys.stderr)
            db = PostgresDatabase(schema)
    return db
