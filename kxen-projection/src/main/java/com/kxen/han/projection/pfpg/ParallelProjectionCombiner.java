package com.kxen.han.projection.pfpg;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.kxen.han.projection.hadoop.writable.TransactionTree;

public class ParallelProjectionCombiner 
extends Reducer<IntWritable, TransactionTree, IntWritable, TransactionTree> {
	@Override
	public void reduce(IntWritable key, Iterable<TransactionTree> values, 
			Context context) throws IOException, InterruptedException {
		TransactionTree tree = new TransactionTree();
		for (TransactionTree tt : values) {
			for (Pair<List<Long>, Long> transac : tt) {
				tree.insertTransac(transac.getLeft(), transac.getRight().intValue());
			}
		}
		context.write(key, tree.getCompressedTree());
	}
}