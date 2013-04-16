package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ParallelCountingMapperReducerTest {
	
	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	
	@Test
	public void testMapper() {
		Text value = new Text("100\t23456\t15.0");
		new MapDriver<LongWritable, Text, Text, LongWritable>()
		.withMapper(new ParallelCountingMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("23456"), new LongWritable(1))
		.runTest();
	}
	
	@Test
	public void testReducer() {
		new ReduceDriver<Text, LongWritable, Text, LongWritable>()
		.withReducer(new ParallelCountingReducer())
		.withInputKey(new Text("22222"))
		.withInputValues(Arrays.asList(new LongWritable(80), new LongWritable(8)))
		.withOutput(new Text("22222"), new LongWritable(88))
		.runTest();
	}
}
