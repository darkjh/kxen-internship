package com.kxen.han.projection.pfpg;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.kxen.han.projection.hadoop.writable.TransactionTree;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

/**
 * Generate group-dependent transactions from a real transaction
 * 
 * @author Han JU
 *
 */
public class ParallelProjectionMapper 
extends Mapper<LongWritable, TransactionWritable, IntWritable, TransactionTree> {
	
	private static final Logger log = 
			LoggerFactory.getLogger(ParallelProjectionMapper.class);
	
	private int groupCount = 0;
	private long lenCount = 0;
	private int numGroup;
	private int maxPerGroup;
	private Map<Long, Long> freq;
	
	private Ordering<Long> byDescFrequencyOrdering = new Ordering<Long>() {
		// reversed order!
		// Array.sort() returns descending ordering
		@Override
		public int compare(Long left, Long right) {
			int freqComp = freq.get(right).compareTo(freq.get(left));
			// fixed order !!!
			return freqComp != 0 ? freqComp : right.compareTo(left);
		}
	};
	
	public static int getGroupByMaxPerGroup(int item, int maxPerGroup) {
		return item / maxPerGroup;
	}
	
	public static int getGroup(int item, int numGroup) {
		return item % numGroup;
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		numGroup = Integer.parseInt(conf.get(ParallelProjection.NUM_GROUP));
		maxPerGroup = conf.getInt(ParallelProjection.MAX_PER_GROUP, 100);
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
	public void map(LongWritable key, TransactionWritable value, Context context)
			throws IOException, InterruptedException {
		ArrayList<Long> items = Lists.newArrayList();
		for (Long item : value) {
			if (freq.containsKey(item)) {
				items.add(item);
			}
		}
		Collections.sort(items, byDescFrequencyOrdering);
		
		IntOpenHashSet processed = IntOpenHashSet.newInstance();
		// generate and output group-dependent transaction
		// go through list in reverse order
		for (int i = items.size()-1; i >= 0 && processed.size() <= numGroup; i--) {
//			int group = getGroupByMaxPerGroup(items.get(i).intValue(), maxPerGroup);
			int group = getGroup(items.get(i).intValue(), numGroup);
			if (!processed.contains(group)) {
				processed.add(group);
				List<Long> subTransac = items.subList(0, i+1); // include
				context.write(new IntWritable(group), new TransactionTree(subTransac, 1l));
				groupCount++;
				lenCount += subTransac.size();
			}
		}
		log.info("Generated {} groups ...", processed.size());
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		log.info("Generated {} group-dependent transactions ...", groupCount);
		log.info("Average length of group-dependent transactions is {} ...", 
				lenCount / groupCount);
		super.cleanup(context);
	}
}