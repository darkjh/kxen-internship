import os, sys

file = sys.argv[1]

with open(file, 'r') as f:
    for line in f:
        s1, s2, c = line.split()

        if long(s1) < long(s2):
            print '\t'.join([s1, s2, c])
        else:
            print '\t'.join([s2, s1, c])

f.close()
