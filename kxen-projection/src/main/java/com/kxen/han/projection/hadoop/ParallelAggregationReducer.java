package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kxen.han.projection.hadoop.writable.GraphLinksWritable;

/**
 * Aggregate the ouput of parallel projection, avoid redundancy
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
		Set<Integer> seen = new HashSet<Integer>();
		String keyStr = key.toString();
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
//				if (!seen.contains(other)) {
//					seen.add(other);
					String out = keyStr+"\t"+other
							+"\t"+glw.getSupport(i);
					context.write(NullWritable.get(), new Text(out));	
//				}
			}
		}
	}
}