import sys

correct = sys.argv[1]
fp = sys.argv[2]

ref = dict()
with open(fp) as f:
    for line in f:
        s1, s2, c = line.split()
        assert s1 != s2
        ref[(s1,s2)] = int(c)
    f.close()


missing = 0
wrong = 0
unknown = 0
with open(correct) as ff:
    for line in ff:
        s1, s2, c = line.split()
        if (s1,s2) not in ref:
            missing += 1
        elif ref[(s1,s2)] != int(c):
            wrong += 1

print "Missing link: " + str(missing)
print "Wrong link: " + str(wrong)


