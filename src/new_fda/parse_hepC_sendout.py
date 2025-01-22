import re

patt = r"HCV RNA, QUANTITATIVE REAL [TIME]* (.*)   NOT DETECTED IU/mL"
patt = re.compile(patt)
def parse(results):
    s = results.split("\n")[2]
    m = patt.match(s)
    if m:
        return m.group(1).strip()
    
#extract = df['comments'].apply(parse)
