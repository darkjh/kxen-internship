# Transform KAR sample dataset to a mahout compliant version

# retrieve customer list
cat original | cut -f 1 | sort -n | uniq > customers

# retrieve item list
cat original | cut -f 2 | sort | uniq > items

# construct an numbered items list
cat items | nl -s $'\t' -b a | sed -s "s/^\s*//" > numbered_items

# join customer list and numbered items list
# construct a mahout compliant dataset
join -t $'\t' -1 2 -2 2 numbered_items <(sort -t $'\t' -k2 original) | awk 'BEGIN { FS = "\t" }; { print $3 "\t" $2 }' > transaction
