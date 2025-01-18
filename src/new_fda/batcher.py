from collections import defaultdict
from psycopg2.extras import execute_values


class BatchUpdates:
    def __init__(self, connection, batch_size=128):
        self.initialize_query_collection()
        self.batch_size = batch_size
        self.connection = connection

    def initialize_query_collections(self):
        self.values_for_queries = defaultdict(list)

    def add_update(self, query, values):
        self.values_for_queries[query].append(values)
        if len(self.values_for_queries[query]) >= self.batch_size:
            self.flush(query)

    def flush_all(self):
        for 
    
