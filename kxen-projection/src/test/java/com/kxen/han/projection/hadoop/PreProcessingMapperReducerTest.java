package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.kxen.han.projection.hadoop.writable.TransactionWritable;
import com.kxen.han.projection.pfpg.PreProcessingMapper;
import com.kxen.han.projection.pfpg.PreProcessingReducer;

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
		new ReduceDriver<Text, Text, LongWritable, TransactionWritable>()
		.withReducer(new PreProcessingReducer())
		.withInputKey(new Text("22222"))
		.withInputValues(Arrays.asList(new Text("1"), new Text("2")))
		.withOutput(new LongWritable(22222l), new TransactionWritable(Arrays.asList(new Long[]{1L, 2L})))
		.runTest();
	}
}
