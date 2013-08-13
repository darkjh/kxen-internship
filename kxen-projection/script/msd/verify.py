import sys
import os

file = sys.argv[1]
prefix = '/home/port/datasets/million-song/'
output = '/home/port/outputs/million-songs/'

empty_recomm = 0
wrong_num_recomm = 0
with open(output + sys.argv[1]) as f:
    for line in f:
        list = line.strip().split('\t')
        assert len(list) == 2 or len(list) == 1
        if len(list) == 1:
            empty_recomm += 1
        else:
            if len(list[1].split()) != 500:
                wrong_num_recomm += 1

print "Empty recommendation user: " + str(empty_recomm)
print "Users with less than 500 recommendations: " + str(wrong_num_recomm)
