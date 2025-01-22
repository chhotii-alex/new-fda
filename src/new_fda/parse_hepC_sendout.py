import re

patt = r"HCV RNA, QUANTITATIVE REAL [TIME]* (.*)   NOT DETECTED IU/mL"
patt = re.compile(patt)
def parse_sendout(results):
    try:
        s = results.split("\n")[2]
    except:
         return None
    m = patt.match(s)
    if m:
        substr = m.group(1).strip()
    else:
         return None
    words = substr.split()
    factor = 1
    if words[0] == "<":
        factor = 1/2
        words = words[1:]
    elif words[0] == ">":
        factor = 2
        words = words[1:]
    try:
        num = float(words[0])
    except:
            return None
    return num*factor

