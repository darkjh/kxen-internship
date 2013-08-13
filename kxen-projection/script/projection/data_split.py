import sys

origin = sys.argv[1]
ouput_folder = sys.argv[2]

history = dict()
with open(origin, 'r') as f:
    for line in f:
        u, s, _ = line.strip().split()
        if history.has_key(u):
            history[u].append(s)
        else:
            history[u] = [s]
    f.close()


for u, ss in history:

