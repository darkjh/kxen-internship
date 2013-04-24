package com.kxen.han.projection.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ParallelAggregationMapperReducerTest {

	@Test
	public void testReducer() {
		new ReduceDriver<IntWritable, GraphLinkWritable, NullWritable, Text>()
		.withReducer(new ParallelAggregationReducer())
		.withInputKey(new IntWritable(1))
		.withInputValues(Arrays.asList(
				new GraphLinkWritable(1, 3, 10), 
				new GraphLinkWritable(1, 3, 10)))
		.withOutput(NullWritable.get(), new Text("1\t3\t10"))
		.runTest();
	}
}
