package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 * {@link com.kxen.han.projection.hadoop.PreProcessingMapper}
 * 
 * @author Han JU
 *
 */
public class PreProcessingReducer 
extends Reducer<Text, Text, Text, Text>{
	
	private static Joiner joiner = Joiner.on(" ").skipNulls();
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Set<String> items = Sets.newHashSet();
		for (Text item: values) {
			items.add(item.toString());
		}
		String out = joiner.join(items);
		context.write(key, new Text(out));
	}
}