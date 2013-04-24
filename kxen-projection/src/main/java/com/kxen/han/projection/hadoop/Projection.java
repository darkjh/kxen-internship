package com.kxen.han.projection.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Maps;
import com.kxen.han.projection.fpg2.FPTree;

public class Projection {
	public static void Project(
			Iterable<TransactionWritable> transacs,
			int minSupport,
			Context context) throws IOException, InterruptedException {
		// construct FP-Tree
		FPTree fpt = new FPTree();
		for (TransactionWritable tw : transacs)
			fpt.insertTransac(tw);
		fpt.clean();

		
		// projection
		int[] headerTableItems = fpt.getHeaderTableItems();
		for (int item : headerTableItems) {
			HashMap<Integer, Long> counter = Maps.newHashMap();
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
					if (item < other)
						context.write(new IntWritable(item), 
								new GraphLinkWritable(item, other, pairSupport));
					else
						context.write(new IntWritable(other), 
								new GraphLinkWritable(other, item, pairSupport));
				}
			}
		}
	}
}
