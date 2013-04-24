package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelProjectionReducer 
extends Reducer<IntWritable, TransactionWritable, IntWritable, GraphLinkWritable> {
	
	int minSupport;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		minSupport = Integer.parseInt(conf.get(ParallelProjection.MIN_SUPPORT));
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<TransactionWritable> values,
			Context context) throws IOException, InterruptedException {
			Projection.Project(values, minSupport, context);
	}
}
