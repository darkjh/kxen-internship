package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kxen.han.projection.hadoop.writable.GraphLinksWritable;

/**
 * Aggregate the ouput of parallel projection, keep only the best
 * pair (wrt support) and filter out redundancies
 * 
 * @author Han JU
 *
 */
public class ParallelAggregationReducer
extends Reducer<IntWritable, GraphLinksWritable, NullWritable, Text> {
	@Override
	public void reduce(IntWritable key, 
			Iterable<GraphLinksWritable> values, Context context) 
					throws IOException, InterruptedException {
		Map<Integer, Long> best = new HashMap<Integer, Long>();
		for (GraphLinksWritable glw : values) {
//			int other = glw.getItem2();
//			long bestSoFar = 
//					best.containsKey(other) ? best.get(other) : 0;
//			if (glw.getSupport() > bestSoFar) {
//				best.put(other, glw.getSupport());
//			}
			int size = glw.getSize();
			for (int i = 0; i < size; i++) {
				int other = glw.getOther(i);
				long bestSoFar = 
						best.containsKey(other) ? best.get(other) : 0;
				if (glw.getSupport(i) > bestSoFar) {
					best.put(other, glw.getSupport(i));
				}
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