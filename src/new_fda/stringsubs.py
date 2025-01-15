import re

substitutionstr = {
    '  ': ' ',                                                            # squeeze double spaces
    "O\\'LEARY": 'L',                                                     # troublesome to parse names with apostrophies
    '[*]+': '',                                                           # strings of decorative/emphasizing astrisks add no information
    'TEST DONE AT [A-Z ,-]+\\.': '',                                      # delete lab  location
    'TEST[INGS]* PERFORMED AT [A-Z ,-]+\\.': '',                          # again try to delete lab location
    '\\(REFERENCE RANGE-NEGATIVE\\)\\.': '',                              # we know this
    '^\\.': '',                                                           # delete stray period at start of string
    '\\(BOWDOIN\\)': '',                                                  # this sometimes appears with names 
    'REPORTED TO AND READ BACK BY ([A-Z]+\\.)? ?[A-Z]+([ ,][A-Z]+)': '',  # sentance that is likely to contain a name
    "DR. [A-Z]+\\'S OFFICE": '',                                          # again, a doctor name
    'REPORTED BY PHONE TO ([A-Z]+\\. )?[A-Z,]+( [A-Z]+)?': '',            # more doctor/staff names
    '\\(?\\d\\d/\\d\\d/\\d\\d @ \\d?\\d:\\d\\d ?[AP]M\\)?': '',           # suppress date/time
    '@ \\d?\\d:\\d\\d ?[AP]M ON \\d\\d/\\d\\d/\\d\\d': '',                # date/time another way
    '\\d?\\d/\\d\\d/\\d\\d': '',                                          # ...or just the date
    'AT \\d\\d[AP]M': '',                                                 # ...or just the time
    '.*GROSSLY HEMOLYZED SERUM INTERFER.?S WITH TEST RELIABILITY. TEST CANCELED AND PATIENT CREDITED. PLEASE SUBMIT A NEW SPECIMEN.*': 'GROSSLY HEMOLYZED SERUM INTERFERES WITH TEST RELIABILITY. TEST CANCELED AND PATIENT CREDITED. PLEASE SUBMIT A NEW SPECIMEN.',
    'QUANTITY NOT SUFFICIENT.*': 'QUANTITY NOT SUFFICIENT.',              # trim off extraneous info
    '^POSITIVE BY EIA.*': 'POSITIVE BY EIA',
    '^TEST CANCELL?ED.*': 'TEST CANCELLED',
    '^NEGATIVE BY EIA..*': 'NEGATIVE BY EIA.',
    '^SPECIMEN NOT PROCESSED.*': 'SPECIMEN NOT PROCESSED',
    '^EQUIVOCAL.*': 'EQUIVOCAL',
    '.*QUANTITY NOT SUFFICIENT.*': 'QUANTITY NOT SUFFICIENT',
    '.*TEST CANCELLED.*': 'TEST CANCELLED',
    '^DUPLICATE.*': 'DUPLICATE',
    '^NON.?REACTIVE.*': 'NON-REACTIVE',
    '^REACTIVE = IMMUNE.*': 'REACTIVE = IMMUNE',
    '.*LOG IN ERROR.*': 'TEST CANCELLED',
    'THIS IS A CORRECTED REPORT.*POSITIVE.*PREVIOUSLY REPORTED.*NEGATIVE.*': 'POSITIVE',    # fix up corrected reports to JUST have result
    'THIS IS A CORRECTED REPORT.*POSITIVE.*PREVIOUSLY REPORTED.*EQUIVOCAL.*': 'POSITIVE',
    'THIS IS A CORRECTED REPORT.*NEGATIVE.*PREVIOUSLY REPORTED.*POSITIVE.*': 'NEGATIVE',
    'THIS IS A CORRECTED REPORT.*NEGATIVE.*PREVIOUSLY REPORTED.*EQUIVOCAL.*': 'NEGATIVE',
    'THIS IS A CORRECTED REPORT.*EQUIVOCAL.*PREVIOUSLY REPORTED.*POSITIVE.*': 'EQUIVOCAL',
    'THIS IS A CORRECTED REPORT.*EQUIVOCAL.*PREVIOUSLY REPORTED.*NEGATIVE.*': 'EQUIVOCAL',
    '.*UNABLE TO [PERFORMLOCATE]+.*': 'UNABLE TO PERFORM',
    '.*TEST.*CANCEL.*': 'TEST CANCELLED',
    '[ACLB]+ ?# ?\\d\\d\\d-?\\d\\d\\d\\d[A-Z]': '',                      # delete accession numbers
    'ON *\\.': '',
    ' \\.': ''
}

substitutions = {re.compile(k) : v for (k, v) in substitutionstr.items() }

def do_substitutions(s):
    for pattern, repl in substitutions.items():
        s = re.sub(pattern, repl, s).strip()
    return s
