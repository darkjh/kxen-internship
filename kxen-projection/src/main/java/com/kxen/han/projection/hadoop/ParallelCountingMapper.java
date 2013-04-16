package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * First step of parallel bipartite graph projection
 * This step counts occurrence of each item, just like the word count example
 * 
 * @author Han JU
 *
 */
public class ParallelCountingMapper 
extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private static LongWritable ONE = new LongWritable(1L);
	private static int ITEM_POSITION = 1;
	// TODO pass separator by configuration
	private static String SEP = "\t";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] triple = value.toString().split(SEP);
		String item = triple[ITEM_POSITION];
		if (item.isEmpty())
			return;
		context.write(new Text(triple[ITEM_POSITION]), ONE);
	}
}
