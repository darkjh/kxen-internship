package com.kxen.han.projection.fpg;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * Bipartite graph projection solution using a FP-Tree approach
 *
 * Construct firstly a FP-Tree, then iterate all frequent items (frequent means its
 * support >= threshold) by ascending frequency order, find and merge its conditional
 * FP-Tree into a single path, then generate all possible frequent pairs
 *
 * @author Han JU
 *
 */
public class Projection {
	private static final Logger log = LoggerFactory.getLogger(Projection.class);
	private static final String SEP = "\t";

    private FPTree fpt;
	private DataModel dataModel;
	private LongIntMap freq;
	private int minSupport;

	private Ordering<Long> byDescFrequencyOrdering = new Ordering<Long>() {
		// reversed order!
		// Array.sort() returns descending ordering
		@Override
		public int compare(Long left, Long right) {
			int freqComp = freq.get(right) - freq.get(left);
			// fixed order !!!
			return freqComp != 0 ? freqComp : right.compareTo(left);
		}
	};

	public Projection(DataModel model, int minSupport) {
		dataModel = model;
		this.minSupport = minSupport;
	}

	/**
	 * First pass of data, construct L, a list of frequent item
	 * in descending order of their frequency
	 */
	public void firstScan() throws Exception {
		freq = LongIntOpenHashMap.newInstance();

		LongPrimitiveIterator itemIter = dataModel.getItemIDs();
		while(itemIter.hasNext()) {
			long itemID = itemIter.next();
			int count = dataModel.getNumUsersWithPreferenceFor(itemID);

			if (count >= minSupport) {
				freq.put(itemID, count);
			}
		}
	}

	/**
	 * Examine every transaction (or every user's history), sort items in
	 * descending frequency order and filter out infrequent item. Then add
	 * this list of items to the FP-tree
	 *
	 */
	private void constructTree() throws Exception {
        fpt = new FPTree();

		LongPrimitiveIterator transIter = dataModel.getUserIDs();
		long count = 0;

		// for every transaction
		while (transIter.hasNext()) {
			if (++count % 5000 == 0)
				log.info("Processed {} transactions ...", count);
			Long transID = transIter.next();
			List<Long> sortedItems = Lists.newArrayList();
			// filtered out infrequent item
			for (Long item : dataModel.getItemIDsFromUser(transID)) {
				if (freq.containsKey(item)) {
					sortedItems.add(item);
				}
			}
			// sort its items in descending frequency order
			Collections.sort(sortedItems, byDescFrequencyOrdering);
			
			// insert into tree
			fpt.insertTransac(sortedItems);
		}
		log.info("Finished constructing FP-Tree ...");
		log.info("Created {} tree nodes ...", FPTreeNode.nodeCount);
	}
	
	/** help GC, these are big */
	private void clean() {
		freq = null;
		dataModel = null;
		fpt.clean();
	}

	public void project(String output) throws Exception {
		// construct FP-tree
		firstScan();
		constructTree();
		clean();	// Runtime.getRuntime().gc(); Thread.sleep(15000);
		
		OutputLayer ol = OutputLayer.newInstance(output);
		
		IntObjectMap<FPTreeNode[]> headerTable = fpt.getHeaderTable();
		long cc = 0;

		for (IntObjectCursor<FPTreeNode[]> item : headerTable) {
			if (++cc % 2500 == 0)
				log.info("Projected for {} items/users ...", cc);
			HashMap<Integer, Integer> counter = Maps.newHashMap();
			FPTreeNode list = item.value[0];

			// visit all conditional path of the current item
			// merge them by counting
			FPTreeNode node = list;
			while (node != null) {
				int condSupport = node.getCount();
				FPTreeNode curr = node.getParent();
				while (!curr.isRoot()) {
					Integer currItem = curr.getItem();
					int count = counter.containsKey(currItem) ? counter.get(currItem) : 0;
					counter.put(currItem, count + condSupport);
					curr = curr.getParent();
				}
				node = node.getNext();
			}

			// generate pairs
			for (Integer i : counter.keySet()) {
				int pairSupport = counter.get(i);
				if (pairSupport >= minSupport) {
					String out = item.key < i ?
						item.key + SEP + i.toString()
							: i.toString() + SEP + item.key;
					out = out + SEP + Integer.toString(pairSupport);
					ol.writeLine(out);
				}
			}
		}
		ol.close();
		log.info("Projection finished ...");
	}

	/**
	 * Simple projection client
	 *
	 * @param args input output minSupport
	 */
	public static void main(String[] args) throws Exception {
		Stopwatch sw = new Stopwatch();
		sw.start();
		DataModel dataModel = new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
						new File(args[0]))));
		
		Projection proj = new Projection(dataModel, Integer.parseInt(args[2]));
		
		proj.project(args[1]);
		sw.stop();

		log.info("Projection process finished, used {} ms ...",
				sw.elapsed(TimeUnit.MILLISECONDS));
	}
}
