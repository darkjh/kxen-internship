# Script to calculate MAP@K metric

import sys

def average_precision_at_k(prediction, actual, k=10):
    """Calculate average precision at k given a prediction list
    and a actual list"""

    if len(prediction) > k:
        prediction = prediction[:k]

    score = 0.0
    num_hits = 0.0

    for i,p in enumerate(prediction):
        if p in actual and p not in prediction[:i]:
            num_hits += 1.0
            score += num_hits / (i + 1.0)
    if not actual:
        return 1.0

    return score / min(len(actual), k)

def mean_average_precision_at_k(pred_list, actu_list, k=10):
    """Calculate mean average precision at k between two lists,
    one is a list of prediction items, the other is a list of actual
    items"""

    # same number of users
    assert len(pred_list) == len(actu_list)

    score = 0.0
    for i, (pred, actu) in enumerate(zip(pred_list, actu_list)):
        if i % 5000 == 0:
            print "Evaluated for " + str(i) + " users ..."
        score += average_precision_at_k(pred, actu, k)

    return score / len(pred_list)

if __name__ == '__main__':
    # run evaluation
    # suppose actu_file is a transaction file like (u, i, c)
    pred_file = open(sys.argv[1], 'r')
    actu_file = open(sys.argv[2], 'r')
    k = int(sys.argv[3])

    pred_list = [line.split() for line in pred_file]
    actu_list = [line.split() for line in actu_file]

    map = mean_average_precision_at_k(pred_list, actu_list, k)
    print "----------------"
    print "MAP@" + str(k) + ": " + str(map)
    print "----------------"
