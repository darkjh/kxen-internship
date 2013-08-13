# Parse the test data
# from transation format, (user, item, count)
# to a list of lists of items
# ordering by the canonical user order imposed by Kaggle

import sys

order_file = sys.argv[1]
to_parse_file = sys.argv[2]

with open(to_parse_file, 'r') as f:
    results = dict()
    for line in f:
        user, song, _ = line.split()
        if results.has_key(user):
            results[user].append(song)
        else:
            results[user] = [song]

for i in range(110000):
    user = str(i+1)
    if results.has_key(user):
        print ' '.join(results[user])
