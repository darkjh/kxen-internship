package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ParallelCountingMapperReducerTest {
	
	@Test
	public void testMapper() {
		Text value = new Text("300	b f h j o");
		new MapDriver<LongWritable, Text, Text, LongWritable>()
		.withMapper(new ParallelCountingMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("b"), new LongWritable(1))
		.withOutput(new Text("f"), new LongWritable(1))
		.withOutput(new Text("h"), new LongWritable(1))
		.withOutput(new Text("j"), new LongWritable(1))
		.withOutput(new Text("o"), new LongWritable(1))
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