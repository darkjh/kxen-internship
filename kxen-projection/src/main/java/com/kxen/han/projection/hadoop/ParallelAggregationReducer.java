package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelAggregationReducer
extends Reducer<IntWritable, GraphLinkWritable, NullWritable, Text> {
	@Override
	public void reduce(IntWritable key, 
			Iterable<GraphLinkWritable> values, Context context) 
					throws IOException, InterruptedException {
		Map<Integer, Long> best = new HashMap<Integer, Long>();
		for (GraphLinkWritable glw : values) {
			int other = glw.getItem2();
			long bestSoFar = 
					best.containsKey(other) ? best.get(other) : 0;
			if (glw.getSupport() > bestSoFar) {
				best.put(other, glw.getSupport());
			}
		}
		
		String keyStr = key.toString();
		for (Integer other : best.keySet()) {
			String out = keyStr+"\t"+other
					+"\t"+best.get(other);
			context.write(NullWritable.get(), new Text(out));			
		}
	}
}
