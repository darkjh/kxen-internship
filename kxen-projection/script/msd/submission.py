import sys
import os

file = sys.argv[1]
hadoop = sys.argv[2]
home = '/home/hadoop/han'
prefix = home + '/datasets/msd-small/'
output = home + '/output/'

# Arrange Mahout output in Kaggle submission format
# If it finds some empty recommendation entry, it will fill with popular songs

# load the triplets and compute song counts
print "Finding popular songs"
with open(prefix + 'kaggle_visible_evaluation_triplets.txt', 'r') as f:
    song_to_count = dict()
    for line in f:
        _, song, _ = line.strip().split('\t')
        if song in song_to_count:
            song_to_count[song] += 1
        else:
            song_to_count[song] = 1
    f.close()

# sort by popularity
songs_ordered = sorted(song_to_count.keys(),
                       key=lambda s: song_to_count[s],
                       reverse=True)
songs_ordered = map(lambda i: str(i), songs_ordered)

# canonical user order
print "Reading canonical user list"
with open(prefix + 'kaggle_users.txt', 'r') as f:
    canonical_users = map(lambda line: line.strip(), f.readlines())
    f.close()

# user to index map
print "Reading user to index map"
with open(prefix + 'users', 'r') as f:
    user_to_index = dict(map(lambda line: line.strip().split('\t')[::-1], f.readlines()))
    f.close()

# reading recommendation results
print "Reading recommendation results"
with open(file, 'r') as f:
    results = dict()
    for line in f:
        list = line.strip().split('\t')

        if hadoop == 'true':
            if len(list) == 2:
                ll = map(lambda i: i.split(':')[0], list[1].strip('[]').split(','))
                list[1] = ' '.join(ll)
                # list[1] = ' '.join(list[1].strip('[]').split(',').map(lambda i: i.split(':')[0]))

        if len(list) == 2:
            results[list[0]] = list[1]
        else:
            results[list[0]] = ' '.join(songs_ordered[0:500])

assert len(results) == 110000

print "Generating submission file"
with open(output + 'submission.txt', 'w') as f:
    for user in canonical_users:
        f.write(results[user_to_index[user]][:] + '\n')
    f.close()
