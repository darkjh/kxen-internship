package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class PreProcessingMapperReducerTest {
	
	@Test
	public void testMapper() {
		Text value = new Text("300\tb\t15");
		new MapDriver<LongWritable, Text, Text, Text>()
		.withMapper(new PreProcessingMapper())
		.withInput(new LongWritable(1), value)
		.withOutput(new Text("300"), new Text("b"))
		.runTest();
	}
	
	@Test
	public void testReducer() {
		new ReduceDriver<Text, Text, Text, TransactionWritable>()
		.withReducer(new PreProcessingReducer())
		.withInputKey(new Text("22222"))
		.withInputValues(Arrays.asList(new Text("1"), new Text("2")))
		.withOutput(new Text("22222"), new TransactionWritable(Arrays.asList(new Long[]{1L, 2L})))
		.runTest();
	}
}
