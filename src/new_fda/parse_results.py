import pandas as pd

misspellings = {}
with open("misspellings.txt", "r") as f:
    while True:
        line = f.readline().strip().split()
        if not len(line):
            break
        misspellings[line[0]] = line[1]

def get_important_words():
    df = pd.read_csv('important.csv', header=None)
    words = set(df.loc[(df[1] == 'y'), 0])
    return words

def read_meanings():
    df = pd.read_csv("meanings.csv")
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
    if c in "./():;<>=":
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

