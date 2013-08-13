# Script for converting million songs dataset to a specific format
import sys

user_index_file = sys.argv[1]
item_index_file = sys.argv[2]
transac_file = sys.argv[3]

user_index = {}
item_index  = {}
with open(user_index_file, 'r') as f:
    for line in f:
       (key, val) = line.split()
       user_index[key] = val

with open(item_index_file, 'r') as f:
    for line in f:
       (key, val) = line.split()
       item_index[key] = val

f = open(transac_file, 'r')
for line in f:
       (user, item, count) = line.split()
       if user in user_index and item in item_index:
           print user_index[user], "\t", item_index[item], "\t", count
