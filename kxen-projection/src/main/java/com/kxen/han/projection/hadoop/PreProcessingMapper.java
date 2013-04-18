package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Pre-processing step, transform the triple format input into a transaction
 * format which is required for parallel projection
 * 
 * @author Han JU
 *
 */
public class PreProcessingMapper 
extends Mapper<LongWritable, Text, Text, Text> {
	private static final String SEP = "\t";
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] triple = value.toString().split(SEP);
		String user = triple[0];
		String item = triple[1];
		if (!item.isEmpty())
			context.write(new Text(user), new Text(item));
	}
}