package com.kxen.han.projection.fpg;

import java.util.Collections;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntLongOpenHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.google.common.collect.Lists;

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

	private FPTreeNode root;
	private IntObjectOpenHashMap<FPTreeNode[]> headerTable;
	
	
	public FPTree(IntLongOpenHashMap freq) 
			throws Exception {
		root = new FPTreeNode();
		headerTable = IntObjectOpenHashMap.newInstance();
		
		firstScan();
		constructTree();
		clean();
	}
	
	public IntObjectMap<FPTreeNode[]> getHeaderTable() {
		return headerTable;
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
	
	public void insertTransac(Iterable<Long> transac, int support) {
		
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
			int item = sorted.get(0).intValue();
			FPTreeNode[] headerList;
			if (headerTable.containsKey(item)) {
				headerList = headerTable.lget();
			} else {
				headerList = new FPTreeNode[]{null, null};
				headerTable.put(item, headerList);
			}
			FPTreeNode next = curr.addChild(item, headerList);
			insertTree(sorted.subList(1, sorted.size()), next);
		}
	}
	
	/** help GC, these are big */
	private void clean() {
		freq = null;
		dataModel = null;
		FPTreeNode.children = null;
	}
}
