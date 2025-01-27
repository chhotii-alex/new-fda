"""
Fetch data from Census BUreau for figuring out SES from home address.
"""
import warnings
import os
import sys
import re
import pickle
from bidict import bidict
import requests

def census_rows_to_dictionaries(data):
    fields = data[0]
    dictionaries = []
    for j in range(1, len(data)):
        d = {}
        for i in range(len(fields)):
            d[fields[i]] = data[j][i]
        dictionaries.append(d)
    return dictionaries

class CensusQuerier:
    def __init__(self, pickle_directory=".", flush_count=50):
        self.zip_pattern = re.compile(r"\d\d\d\d\d")
        self.census_api = "https://api.census.gov/data"
        self.zcta_symbol = "zip%20code%20tabulation%20area"
        self.year = 2020
        self.key_file_path = "census_key.txt"
        self.api_key = None # will be read in when needed
        # See https://data.census.gov/cedsci/profile?g=860XX00US02163 for links to various data
        #   tables.
        # See https://api.census.gov/data/2020/acs/acs5/subject/groups/S0101.html for variables in
        #   S0101.
        # Very small subset of the variables here:
        self.var_lookup = bidict(
            {"Median Household Income": "S1901_C01_012E",
             "Total Population": "S0101_C01_001E",
             }
        )
        self.pickle_directory = pickle_directory
        self.pickle_file_name = "zipcodes.pkl"
        self.zipcache = None
        self.miss_count = 0
        self.flush_count = flush_count

    def __del__(self):
        self.cache_data()

    def get_api_key(self):
        if self.api_key is None:
            # The file named census_key.txt contains (only) the key for accessing the Census API.
            # This file is not archived to the the git repository; putting api keys into github
            # is bed practice, so I'm .gitignoring it.
            # The Census Bureau permits a small number of queries per some time period without the
            # use of a key, so one can so some basic testing. However, for issuing queries in the
            # bulk quantity that we require, one needs an API key.
            # Fortunately, getting an API key is easy and free.
            # Google "how to get a census api key" for the pointers if you need to run this code
            # on another machine and I'm not around to give you the exisitng key.
            with open(self.key_file_path, "r") as f:
                self.api_key = f.readline().strip()
        return self.api_key

    def fetch_vars_for_zcta(self, zipcode):
        """
           Raises:
           -- ValueError - invalid zipcode or variable
           -- RuntimeError  - other error (i.e. census API is down)
        """
        vars = ",".join(self.var_lookup.values())
        url = "%s/%d/acs/acs5/subject?get=NAME,%s&for=%s:%s&key=%s" % (
            self.census_api,
            self.year,
            vars,
            self.zcta_symbol,
            zipcode,
            self.get_api_key(),
        )
        r = requests.get(url)
        if r.status_code == 200:
            as_json = r.json()
            z = census_rows_to_dictionaries(as_json)
            return z[0]
        elif r.status_code == 204:
            raise ValueError
        else:
            raise RuntimeError

    def get_pickle_path(self):
        return os.path.join(self.pickle_directory, self.pickle_file_name)

    def get_backup_pickle_path(self):
        return os.path.join(self.pickle_directory, "bak_" + self.pickle_file_name)

    def get_zipcache(self):
        if self.zipcache is None:
            if os.path.exists(self.get_pickle_path()):
                with open(self.get_pickle_path(), 'rb') as f:
                    self.zipcache = pickle.load(f)
            else:
                self.zipcache = {}
        return self.zipcache

    def parameter_for_zip(self, zipcode, var):
        """
           Raises:
           -- RuntimeError - if API call could not be made
           Tries 3 times before giving up (API does randomly glitch sometimes.)
           If the 3rd time glitches out, then the RuntimeError is raised from here.
        """
        if var not in self.var_lookup:
            return None
        if zipcode not in self.get_zipcache():
            for _ in range(3):
                runtime_error = None
                try:
                    self.zipcache[zipcode] = self.fetch_vars_for_zcta(zipcode)
                    break
                except ValueError: # presumably a bad zipcode
                    self.zipcache[zipcode] = None
                    break
                except RuntimeError as r:
                    runtime_error = r
                    continue
            if runtime_error is not None:
                raise runtime_error
            self.miss_count += 1
            if self.miss_count > self.flush_count:
                self.cache_data()
        d = self.zipcache[zipcode]
        if d:
            return d[self.var_lookup[var]]
        else:
            return None

    def cache_data(self):
        if not self.zipcache:
            return
        if not self.miss_count:
            return
        if os.path.isfile(self.get_pickle_path()):
            os.replace(self.get_pickle_path(), self.get_backup_pickle_path())
        try:
            with open(self.get_pickle_path(), 'wb') as f:
                pickle.dump(self.zipcache, f)
            self.miss_count = 0
        except:
            os.replace(self.get_backup_pickle_path(), self.get_pickle_path())

    def ses_bin_for_zip(self, zipcode, bin_width=26000, min_bin_number=1, max_bin_number=5):
        if zipcode is None:
            return None
        zipcode = str(zipcode)
        zipcode = zipcode.zfill(5)
        if len(zipcode) > 5:
            zipcode = zipcode[:5]
        if not self.zip_pattern.match(zipcode):
            return None
        med_income = self.parameter_for_zip(zipcode, "Median Household Income")
        if med_income is not None:
            med_income = float(med_income)
            if med_income > 0:
                bin_number = int(med_income/ bin_width)
                if bin_number > max_bin_number:
                    return max_bin_number
                elif bin_number < min_bin_number:
                    return min_bin_number
                else:
                    return bin_number
        return None

# fail fast if API key not found
dummy = CensusQuerier()
dummy.get_api_key()
