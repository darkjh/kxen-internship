#!/bin/bash

hadoop=/home/port/hadoop-1.1.2/bin/hadoop
msd_file=dataset/msd/msd_train_transposed
output_file=output/giraph/msd_transposed

total_mem=10240
opts=-Xmx
meg=m
gig=g

echo "Begin Giraph Benchmark on MSD user space ..."
for i in 16 20 24 28 32 36 40 44 48 52 56 60 63; do
    # calculate heap size to allocate
    proc=`expr $i / 4`
    mem=`expr $total_mem / $proc`
    # call the Giraph job
    $hadoop jar ../target/kxen-projection-jar-with-dependencies.jar com.kxen.han.projection.giraph.GiraphProjection -Dmapred.child.java.opts=$opts$mem$meg -Ddfs.replication=1 -i $msd_file -o $output_file -tmp output/tmp -w $i -g 199 -s 2 -startFrom 2

    if [ $? != 0 ]; then
	echo "Error ..."
	# break;
    fi

    # after a job, delete the output folder
    $hadoop dfs -rmr $output_file
done

echo "Benchmark finished ..."