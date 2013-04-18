package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private static LongWritable ONE = new LongWritable(1L);
	private static int ITEMS_POSITION = 1;
	private static String SEP = "\t";
	private static String ITEM_SEP = " ";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split(SEP);
		String[] items = line[ITEMS_POSITION].split(ITEM_SEP);
		
		for (String item : items) {
			if (item.isEmpty())
				continue;
			context.write(new Text(item), ONE);
		}
	}
}