package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

/**
 * {@link com.kxen.han.projection.hadoop.PreProcessingMapper}
 * 
 * @author Han JU
 *
 */
public class PreProcessingReducer 
extends Reducer<Text, Text, LongWritable, TransactionWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		long k = Long.parseLong(key.toString());
		Set<Long> items = Sets.newHashSet();
		for (Text item: values) {
			items.add(Long.parseLong(item.toString()));
		}
		context.write(new LongWritable(k), 
				new TransactionWritable(Arrays.asList(items.toArray(new Long[]{}))));
	}
}