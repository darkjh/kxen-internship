#!/bin/bash

hadoop=/home/port/hadoop-1.1.2/bin/hadoop

echo "Begin Giraph Benchmark on MSD item space ..."
for i in 4 8 16 32 63; do
    # call the Giraph job
    $hadoop jar ../target/kxen-projection-jar-with-dependencies.jar com.kxen.han.projection.giraph.GiraphProjection -Dmapred.child.java.opts=-Xmx1g -Ddfs.replication=1 -i dataset/msd/msd_train_data -o output/giraph/msd_full -tmp output/tmp -w $i -g 19 -s 2 -startFrom 2

    if [ $? != 0 ]; then
	echo "Error ..."
	break;
    fi

    # after a job, delete the output folder
    $hadoop dfs -rmr output/giraph/msd_full
done

echo "Benchmark finished ..."