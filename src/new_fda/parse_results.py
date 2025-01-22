import re
import importlib.resources
import pandas as pd

misspellings = {}
with importlib.resources.open_text("new_fda", "misspellings.txt") as f:
    while True:
        line = f.readline().strip().split()
        if not len(line):
            break
        misspellings[line[0]] = line[1]

def get_important_words():
    with importlib.resources.open_text("new_fda", "important.csv") as f:
        df = pd.read_csv(f, header=None)
    words = set(df.loc[(df[1] == 'y'), 0])
    return words

def read_meanings():
    with importlib.resources.open_text("new_fda", "meanings.csv") as f:
        df = pd.read_csv(f)
    df.fillna('', inplace=True)
    df['abbrev'] = df['abbrev'].str[:20]
    return dict(zip(df["abbrev"], df["meaning"]))

important = get_important_words()
meanings = read_meanings()

def subs(c, second_pass):
    if c.isalpha():
        return c
    if c.isdigit():
        if second_pass:
            return "9"
        else:
            return c
    if c in "./():;<>":
        return c
    return " "

def substitutions(s, second_pass):
    t = ''.join([subs(c, second_pass) for c in s])
    if second_pass:
        t = " ".join([word.rstrip(".") for word in t.split()])
    def strip_start_period(word):
        p = word.lstrip(".")
        if len(p) and not p[0].isdigit():
            word = p
        return word
    t = " ".join([strip_start_period(word) for word in t.split()])
    def replace_misspellings(word):
        if word in misspellings:
            return misspellings[word]
        return word
    t = " ".join([replace_misspellings(word) for word in t.split()])
    return t

def abbrev_result(s):
    s = s.upper().replace('..', '.')
    s = substitutions(s, False)
    words = s.split()
    important_words = [word for word in words if substitutions(word, True) in important]
    news = ' '.join(important_words)
    return news


def extract_result(s):
    news = abbrev_result(s)[:20]
    if news in meanings:
        meaning = meanings[news]
    else:
        print(news)
        meaning = 'unknown'
    return meaning

def parse_numeric_from_free(comm):
    t = comm.upper().strip()
    t = re.sub(r"\(.*\)", "", t)
    t = re.sub(r"THIS IS A CORRECTED REPORT.*CHANGED TO", "", t)
    t = re.sub(r"\d\d?/\d\d?/\d\d(\d\d)?.?", "", t)
    t = re.sub(r"\d?\d[:\.]\d\d([AP]M)?\.?", "", t)
    t = re.sub(r"CHANGED FROM .* TO", "", t)
    t = re.sub(r"^DETECTED", "", t)
    for bad in [
            "HIV-1 RNA DETECTED",
            ",",
            "HCV RNA DETECTED",
            "THIS IS AN ADDITIONAL REPORT .",
            "HBV DNA DETECTED",
            "@",
            "THIS IS A CORRECTED REPORT",
            "HCV VIRAL LOAD END-POINT DETERMINATION.",
    ]:
        t = t.replace(bad, "")
    for orig, subs in [
            ("*", " "),
            ("GREATER THAN", " > "),
            ("LESS THAN", " < "),
            ("OOO", "000"),
    ]:
        t = t.replace(orig, subs)
    for item in [
            'IU',
            "COPIES",
            "ML",
            "<",
            ">"
            "COP",
            ">",
            "COP",
    ]:
        t = t.replace(item, " %s " % item)
    t = t.strip(". AP:")
    if not len(t):
            return None
    words = t.split()
    factor = 1
    if words[0] == "<":
        factor = 1/2
        words = words[1:]
    elif words[0] == ">":
        factor = 2
        words = words[1:]
    try:
        num = float(words[0])
        # Check that the next thing after the number is a unit:
        if ("COP" not in words[1]) and ("IU" not in words[1]):
            return None
    except:
            return None
    return num*factor

