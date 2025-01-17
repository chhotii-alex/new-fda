import pandas as pd
import hashlib
from pathlib import Path
import pickle

class ResultsCacher:
    def __init__(self, db):
        self.name = db.get_name()
        self.get_folder()

    def get_folder(self):
        path = Path(".") / "cache" / self.name
        if not path.exists():
            path.mkdir(parents=True)
        return path

    def has_results(self, query_text):
        return self.path_for_query(query_text).exists()

    def filename_for_query(self, query_text):
        h = hashlib.new('sha256')
        h.update(query_text.encode(encoding='utf-8', errors='replace'))
        s = h.hexdigest()
        filename = "%s.pkl" % s
        return filename
    
    def path_for_query(self, query_text):
        return self.get_folder() / self.filename_for_query(query_text)

    def save_results(self, query_text, results):
        with open(self.path_for_query(query_text), "wb") as f:
            pickle.dump(results, f)

    def results(self, query_text):
        with open(self.path_for_query(query_text), "rb") as f:
            results = pickle.load(f)
            return results

