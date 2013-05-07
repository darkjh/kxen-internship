package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.kxen.han.projection.hadoop.writable.TransactionWritable;

public class ParallelCountingMapperReducerTest {
	
	@Test
	public void testMapper() {
		TransactionWritable value = 
				new TransactionWritable(Arrays.asList(new Long[]{1L,2L,3L,4L,5L}));
		new MapDriver<LongWritable, TransactionWritable, LongWritable, LongWritable>()
		.withMapper(new ParallelCountingMapper())
		.withInput(new LongWritable(300l), value)
		.withOutput(new LongWritable(1L), new LongWritable(1))
		.withOutput(new LongWritable(2L), new LongWritable(1))
		.withOutput(new LongWritable(3L), new LongWritable(1))
		.withOutput(new LongWritable(4L), new LongWritable(1))
		.withOutput(new LongWritable(5L), new LongWritable(1))
		.runTest();
	}
	
	@Test
	public void testReducer() {
		new ReduceDriver<LongWritable, LongWritable, LongWritable, LongWritable>()
		.withReducer(new ParallelCountingReducer())
		.withInputKey(new LongWritable(22222L))
		.withInputValues(Arrays.asList(new LongWritable(80), new LongWritable(8)))
		.withOutput(new LongWritable(22222L), new LongWritable(88))
		.runTest();
	}
}