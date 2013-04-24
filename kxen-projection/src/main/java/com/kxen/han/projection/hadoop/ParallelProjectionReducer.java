package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelProjectionReducer 
extends Reducer<IntWritable, TransactionWritable, Text, LongWritable> {
	
	@Override
	public void reduce(IntWritable key, Iterable<TransactionWritable> values,
			Context context) throws IOException, InterruptedException {
		long cc = 0;
		for (TransactionWritable tw : values) {
			
		}
	}
}
