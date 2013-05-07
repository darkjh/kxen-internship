package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.kxen.han.projection.hadoop.writable.TransactionWritable;

/**
 * First step of parallel bipartite graph projection
 * This step counts occurrence of each item, just like the word count example
 * Expect data is in a transaction format:
 * 
 * 	user1	i1, i2, i3 ...
 * 	user2	i2, i5, i1 ...
 * 	...
 * 
 * @author Han JU
 *
 */
public class ParallelCountingMapper 
extends Mapper<LongWritable, TransactionWritable, LongWritable, LongWritable> {
	
	private static LongWritable ONE = new LongWritable(1L);
	
	@Override
	public void map(LongWritable key, TransactionWritable value, Context context)
			throws IOException, InterruptedException {
		for (Long item : value) {
			context.write(new LongWritable(item), ONE);
		}
	}
}