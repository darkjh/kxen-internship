package com.kxen.han.projection.fpg;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * A FP-Tree implementation
 * 
 * Based on article: www.cs.uiuc.edu/~hanj/pdf/sigmod00.pdf
 * 
 * @author Han JU
 *
 */
public class FPTree {
	
	private static final Logger log = LoggerFactory.getLogger(FPTree.class);
	
	private DataModel dataModel;
	private int supportThreshold;
	
	private FPTreeNode root;
	private Map<Integer, FPTreeNode[]> headerTable;
	private Map<Long, Integer> freq;
	
	private Ordering<Long> byDescFrequencyOrdering = new Ordering<Long>() {
		// reversed order! 
		// Array.sort() returns descending ordering
		// TODO bug??
		@Override
		public int compare(Long left, Long right) {
			int freqComp = freq.get(right) - freq.get(left);
			// fixed order !!!
			return freqComp != 0 ? freqComp : (int) (right - left);
		}
	};
	
	public FPTree(String filepath, int threshold) throws Exception {
		this(new File(filepath), threshold);
	}
	
	public FPTree(String filepath, boolean transpose, int threshold) throws Exception {
		this(new File(filepath), transpose, threshold);
	}
	
	public FPTree(File file, int threshold) throws Exception {
		this(new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(file))),
				threshold);
	}
	
	public FPTree(File file, boolean transpose, int threshold) throws Exception {
		this(new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(
						new FileDataModel(file, transpose, Long.MAX_VALUE))),
				threshold);
	}
	
	public FPTree(DataModel model, int threshold) throws Exception {
		dataModel = model;
		supportThreshold = threshold;
		
		root = new FPTreeNode();
		headerTable = Maps.newHashMap();
		
		firstScan();
		constructTree();
		clean();
	}
	
	public Map<Integer, FPTreeNode[]> getHeaderTable() {
		return headerTable;
	}

	/** 
	 * First pass of data, construct L, a list of frequent item
	 * in descending order of their frequency
	 */
	public void firstScan() throws Exception {
		freq = Maps.newHashMap();
		
		LongPrimitiveIterator itemIter = dataModel.getItemIDs();
		while(itemIter.hasNext()) {
			long itemID = itemIter.next();
			int count = dataModel.getNumUsersWithPreferenceFor(itemID);
			
			if (count >= supportThreshold) {
				freq.put(itemID, count);
				headerTable.put((int) itemID, new FPTreeNode[]{null, null});				
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
			insertTree(sortedItems, root);
		}
		log.info("Finished constructing FP-Tree ...");
		log.info("Created {} tree nodes ...", FPTreeNode.nodeCount);
	}
	
	/**
	 * Take a list of items of form [p|P] and a tree node t, recursively add p 
	 * (the header of the list) in the subtree of which t is the root
	 * 
	 * @param sorted
	 * @param curr
	 */
	private void insertTree(List<Long> sorted, FPTreeNode curr) {
		if (!sorted.isEmpty()) {
			Integer item = sorted.get(0).intValue();
			FPTreeNode[] headerList = headerTable.get(item);
			FPTreeNode next = curr.addChild(item, headerList);
			insertTree(sorted.subList(1, sorted.size()), next);
		}
	}
	
	/** help GC, these are big */
	private void clean() {
		freq = null;
		dataModel = null;
	}
	
	public static void main(String[] args) throws Exception {
		// String file = "/home/port/datasets/msd-small/test_triples";
		String file = "src/main/resources/test_triples";
		// String file = "./resources/TestExampleAutoGen";
		Runtime rt = Runtime.getRuntime();

		FPTree fpt = new FPTree(file, 2);

		rt.gc();
		Thread.sleep(20000);
	}
}
