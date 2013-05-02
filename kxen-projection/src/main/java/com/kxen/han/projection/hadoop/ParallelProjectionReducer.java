package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.kxen.han.projection.fpg2.FPTree;

/**
 * FP-Tree approach projection on group-dependent transactions
 * Output frequent pair (projected graph link) of the group, possible redundancy
 * with other groups' results
 * 
 * @author Han JU
 *
 */
public class ParallelProjectionReducer
extends Reducer<IntWritable, TransactionWritable, IntWritable, GraphLinksWritable> {
	
	private static final Logger log = 
			LoggerFactory.getLogger(ParallelProjectionReducer.class);
	
	private int minSupport;
	private int group;
	private int numGroup;

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		minSupport = Integer.parseInt(conf.get(ParallelProjection.MIN_SUPPORT));
		numGroup = Integer.parseInt(conf.get(ParallelProjection.NUM_GROUP));
	}

	@Override
	public void reduce(IntWritable key, Iterable<TransactionWritable> values,
			Context context) throws IOException, InterruptedException {
		log.info("Reduce started ...");
		// construct FP-Tree
		FPTree fpt = new FPTree();
		long cc = 0;
		for (TransactionWritable tw : values)
			cc += fpt.insertTransac(tw);
		fpt.clean();
		log.info("FP-Tree construction finished, created {} nodes ...", cc);

		Map<Integer, List<Pair<Integer, Long>>> results = Maps.newTreeMap();
		group = key.get();

		// projection
		cc = 0;
		int[] headerTableItems = fpt.getHeaderTableItems();
		for (int item : headerTableItems) {
			// only project for items in the current group
			// avoid lots of redundancy
			if (ParallelProjectionMapper.getGroup(item, numGroup) != group)
				continue;
			if (++cc % 1000 == 0)
				log.info("Projected for {} items", cc);
			Map<Integer, Long> counter = Maps.newHashMap();
			int nextNode = fpt.getHeaderNext(item);
			// chase for same item
			while (nextNode > 0) {
				long condSupport = fpt.count(nextNode);
				// go upward
				int curr = fpt.getParent(nextNode);
				while (!fpt.isRoot(curr)) {
					int currItem = fpt.getItem(curr);
					long count = counter.containsKey(currItem) ? counter
							.get(currItem) : 0;
					counter.put(currItem, count + condSupport);
					curr = fpt.getParent(curr);
				}
				nextNode = fpt.getNext(nextNode);
			}

			// generate pairs
			for (Integer other : counter.keySet()) {
				long pairSupport = counter.get(other);
				if (pairSupport >= minSupport) {
					int k, v;
					if (item < other) {
						k = item;
						v = other;
					} else {
						k = other;
						v = item;
					}
					if (!results.containsKey(k))
						results.put(k, new LinkedList<Pair<Integer, Long>>());
					results.get(k).add(Pair.of(v, pairSupport));
				}
			}
		}
		fpt = null;
		log.info("Projection finished, output ...");
		for (Integer item : results.keySet()) {
			context.write(new IntWritable(item), new GraphLinksWritable(item,
					results.get(item)));
		}
	}
}
