package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class ParallelProjectionMapper 
extends Mapper<Text, TransactionWritable, IntWritable, TransactionWritable> {
	
	private int numGroup;
	private Map<Long, Long> freq;
	
	private Ordering<Long> byDescFrequencyOrdering = new Ordering<Long>() {
		// reversed order!
		// Array.sort() returns descending ordering
		@Override
		public int compare(Long left, Long right) {
			int freqComp = freq.get(right).compareTo(freq.get(left));
			// fixed order !!!
			return freqComp != 0 ? freqComp :  right.compareTo(left);
		}
	};
	
	public int getGroup(long item) {
		return (int) (item % numGroup);
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		numGroup = Integer.parseInt(conf.get(ParallelProjection.NUM_GROUP));
		Path[] caches = DistributedCache.getLocalCacheFiles(conf);
		FileSystem fs = FileSystem.getLocal(conf); // cache is stored locally
		Path fListPath = fs.makeQualified(caches[0]);
		ObjectInputStream ois = new ObjectInputStream(fs.open(fListPath));
		try {
			freq = (Map<Long, Long>) ois.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			ois.close();
		}
	}
	
	@Override
	public void map(Text key, TransactionWritable value, Context context)
			throws IOException, InterruptedException {
//		// parse input
//		String[] line = value.toString().split("\t");
//		String[] itemsStr = line[1].split(" ");
		ArrayList<Long> items = Lists.newArrayList();
		for (Long item : value) {
			if (freq.containsKey(item)) {
				items.add(item);
			}
		}
		Collections.sort(items, byDescFrequencyOrdering);
		
		Set<Integer> processed = Sets.newHashSet();
		// generate and output group-dependent transaction
		// go through list in reverse order
		for (int i = items.size()-1; i >= 0; i--) {
			int group = getGroup(items.get(i));
			if (!processed.contains(group)) {
				processed.add(group);
				List<Long> subTransac = items.subList(0, i+1); // include
				context.write(new IntWritable(group), new TransactionWritable(subTransac));
			}
		}
	}
}
