package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ParallelAggregationMapper
extends Mapper<IntWritable, GraphLinkWritable, IntWritable, GraphLinkWritable> {
	
	@Override
	public void map(IntWritable key, GraphLinkWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
