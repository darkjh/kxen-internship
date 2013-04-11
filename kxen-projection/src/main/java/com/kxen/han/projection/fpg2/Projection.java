package com.kxen.han.projection.fpg2;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.kxen.han.projection.fpg.OutputLayer;

// TODO repeated code !
/**
 * 
 * @author Han JU
 * 
 */
public class Projection {

	private static final Logger log = LoggerFactory.getLogger(Projection.class);
	private static final String SEP = "\t";

	private DataModel dataModel;
	private int minSupport;
	private FPTree fpt;
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

	public Projection(DataModel model, int minSupport) {
		dataModel = model;
		this.minSupport = minSupport;
	}

	/**
	 * First pass of data, construct L, a list of frequent item in descending
	 * order of their frequency
	 */
	public void firstScan() throws Exception {
		freq = Maps.newHashMap();

		LongPrimitiveIterator itemIter = dataModel.getItemIDs();
		while (itemIter.hasNext()) {
			long itemID = itemIter.next();
			int count = dataModel.getNumUsersWithPreferenceFor(itemID);

			if (count >= minSupport) {
				freq.put(itemID, count);
			}
		}
	}

	private int insertTransac(FPTree tree, List<Long> transac, long minSupport) {

		int temp = FPTree.ROOTNODEID;
		int ret = 0;
		boolean addCountMode = true;

		for (long item : transac) {
			int child;
			int attribute = (int) item;

			if (addCountMode) {
				child = tree.childWithAttribute(temp, attribute);
				if (child == -1) {
					addCountMode = false;
				} else {
					tree.addCount(child, 1L);
					temp = child;
				}
			}
			if (!addCountMode) {
				child = tree.createNode(temp, attribute, 1L);
				temp = child;
				ret++;
			}
		}
		return ret;
	}

	private void constructTree() throws Exception {
		fpt = new FPTree(freq.size());

		LongPrimitiveIterator transIter = dataModel.getUserIDs();
		long count = 0;
		long nodeCount = 0;

		log.info("Begin constructing FP-tree ...");
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
			nodeCount += insertTransac(fpt, sortedItems, minSupport);
		}
		log.info("Created {} nodes in FP-tree ...", nodeCount);
	}

	private void clean() {
		freq = null;
		dataModel = null;
	}

	public void project(OutputLayer ol) throws Exception {
		// construct FP-tree
		firstScan();
		constructTree();
		clean();
		
		// projection
		long cc = 0;

		int headerTableCount = fpt.getHeaderTableCount();
		int[] headerTableItems = fpt.getHeaderTableAttributes();
		
		// for each frequent item
 		for (int i = 0; i < headerTableCount; i++) {
 			if (++cc % 5000 == 0)
 				log.info("Projected for {} items/users ...", cc);
 			
 			HashMap<Integer, Long> counter = Maps.newHashMap();
 			int item = headerTableItems[i];
 			int nextNode = fpt.getHeaderNext(item);
 			// chase for same item
 			while (nextNode > 0) {
 				long condSupport = fpt.count(nextNode);
 				// go upward
 				int curr = fpt.parent(nextNode);
 				while (curr != fpt.ROOTNODEID) {
 					int currItem = fpt.attribute(curr);
 					long count = counter.containsKey(currItem) ? counter
 							.get(currItem) : 0;
 					counter.put(currItem, count + condSupport);
 					curr = fpt.parent(curr);
 				}
 				nextNode = fpt.next(nextNode);
 			}
 			
			// generate pairs
			for (Integer other : counter.keySet()) {
				long pairSupport = counter.get(other);
				if (pairSupport >= minSupport) {
					String out = item < other ? Integer.toString(item) + SEP
							+ other.toString() : other.toString() + SEP
							+ Integer.toString(item);
					out = out + SEP + Long.toString(pairSupport);
					ol.writeLine(out);
				}
			}
 		}
 		ol.close();
 		log.info("Projection finished ...");
	}

	public static void main(String[] args) throws Exception {
		Stopwatch sw = new Stopwatch();	
		sw.start();
		DataModel dataModel = new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
						new File(args[0]))));
		Projection proj = new Projection(dataModel, Integer.parseInt(args[2]));
		proj.project(new OutputLayer(new File(args[1])));
		sw.stop();
		
		log.info("Projection process finished, used {} ms ...", 
				sw.elapsed(TimeUnit.MILLISECONDS));
	}
}
