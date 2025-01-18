

secret_info = {}
with open("secrets.txt", "r") as f:
    while True:
        line = f.readline().strip()
        if not line:
            break
        fields = [f.strip() for f in line.split(":")]
        secret_info[fields[0]] = int(fields[1])

def get_encode_keys():
    return secret_info['K'], secret_info['M']

def get_decode_keys():
    return secret_info['Inverse of k'], secret_info['M']
        

