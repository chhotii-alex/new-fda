# Much of this code is copied from the shovel repo
import re
import datetime
import pandas as pd
from datetime import date

def get_immunosuppressive_meds():
    medications = ["abatacept", "adalimumab", "anakinra", "azathioprine", "basiliximab", 
               "budesonide", "certolizumab", "cyclosporine", "daclizumab", "everolimus", 
               "etanercept", "golimumab", "infliximab", "ixekizumab", "leflunomide", 
               "lenalidomide", "methotrexate", "mycophenolate", "natalizumab", 
               "pomalidomide", "prednisone", "rituximab", "secukinumab", "serolimus", 
               "tacrolimus", "tocilizumab", "tofacitinib", "ustekinumab", "vedolizumab", 
               "dexamethasone"]
    return medications



int_prefix_pattern = re.compile(r"(\d+)")
fraction_pattern = re.compile(r"(\d*)\s*(\d+)/(\d+)")
number_prefix_pattern = re.compile(r"(\d+\.?\d*)")

def flexible_number_parse(s):
    s = s.strip().lower()
    if s.startswith("take"):
        s = s[4:].strip()
    if s == "one":
        return 1
    elif s == "two":
        return 2
    elif s == "one half":
        return 0.5
    elif s == "half":
        return 0.5
    m = fraction_pattern.match(s)
    if m:
        total = 0
        if len(m.group(1)):
            total += int(m.group(1))
        total += int(m.group(2))/int(m.group(3))
        return total
    m = number_prefix_pattern.match(s)
    if m:
        return float(m.group(1))
    return None

def infer_stop_date(row):
    if not pd.isnull(row["end_dt"]):
        return row["end_dt"]
    start_dt = row["start_date"]
    dispense = row["dispense"]
    if dispense is None:
        return start_dt
    else:
        dispense = dispense.strip()
    m = int_prefix_pattern.match(dispense)
    if not m:
        return start_dt
    dispense_count = int(m.group(1))
    refills = row["refills"]
    if refills is None:
        refill_count = 1
    else:
        m = int_prefix_pattern.match(refills)
        if m:
            refill_count = int(m.group(1)) + 1
        else:
            refill_count = 1
    take_amount = row["take_amount"]
    if take_amount is None:
        return start_dt
    amount_per_day = flexible_number_parse(take_amount)
    if not amount_per_day:
        return start_dt
    days = dispense_count*refill_count/amount_per_day
    days = min(days, 365*5)
    return start_dt + datetime.timedelta(days=days)

def get_meds(virginia):
    now = date(2024, 8, 1)
    columns = ['mrn', 'med_name', 'dispense', 'refills', 'take_amount', 'start_dt  start_date', 'coalesce(stop_dt, discontinue_dt) end_dt', 'duration']
    columns_clause = ", ".join(columns)
    table = virginia.get_prefix() + "vwOMR_Med"
    all_data = None
    for med in get_immunosuppressive_meds():
        where = """med_name like '%s'
                and duration like 'ongoing%s'
                and mrn is not null
                and start_dt < '%s'
                and (stop_dt is null or stop_dt < '%s')
                and (discontinue_dt is null or discontinue_dt < '%s')"""  % (med, '%%', now, now, now)
        query = virginia.order_select_query('', columns_clause, table, None, where)
        print(query)
        df = virginia.do_select(query)
        if df.shape[0] < 1:
            continue
        df['inferred_stop_date'] = df.apply(infer_stop_date, axis=1)
        df['end_date'] = df['inferred_stop_date'] + pd.Timedelta(days=30)
        if all_data is None:
            all_data = df
        else:
            all_data = pd.concat([all_data, df])
    return all_data
