package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Counting of individual supports
 * Suppose that the input is an edge format
 * 
 *   user id <TAB> item id <TAB> (some other values)
 * 
 * @author Han JU
 *
 */
public class TripleCounting {
	
	private static final Pattern SEP = Pattern.compile("\t");
	private static final LongWritable ONE = new LongWritable(1l);
	private static final int PROD = 1;
	private static final int USER = 0;
	
	public static class TripleCountingMapper
	extends Mapper<LongWritable,Text,LongWritable,LongWritable> {
		private LongWritable out = new LongWritable();
		private boolean userSpace = false;
		
		@Override 
		public void setup(Context context) {
			// controls the projection space
			userSpace = context.getConfiguration().getBoolean(
					GiraphProjection.USER_SPACE, false);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = SEP.split(value.toString());
			
			if (userSpace) {
				out.set(Long.parseLong(line[USER]));
			} else {
				out.set(Long.parseLong(line[PROD]));
			}
			context.write(out, ONE);
		}
	}
	
	public static class TripleCountingReducer
	extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
		private LongWritable out = new LongWritable();
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable l : values) {
				sum += l.get();
			}
			out.set(sum);
			context.write(key, out);
		}
	}
}