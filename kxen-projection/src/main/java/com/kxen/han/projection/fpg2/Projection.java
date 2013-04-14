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
	
	private int numThreads = 1;			/* single thread by default */
	
	int headerTableCount;
	int[] headerTableItems;

	private Ordering<Long> byDescFrequencyOrdering = new Ordering<Long>() {
		// reversed order!
		// Array.sort() returns descending ordering
		@Override
		public int compare(Long left, Long right) {
			int freqComp = freq.get(right) - freq.get(left);
			// fixed order !!!
			return freqComp != 0 ? freqComp :  right.compareTo(left);
		}
	};

	public Projection(DataModel model, int minSupport) {
		dataModel = model;
		this.minSupport = minSupport;
	}
	
	public Projection(DataModel model, int minSupport, int numThreads) {
		dataModel = model;
		this.minSupport = minSupport;
		this.numThreads = numThreads;
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
			nodeCount += fpt.insertTransac(sortedItems);
		}
		log.info("Created {} nodes in FP-tree ...", nodeCount);
	}

	/* help gc, these are big */
	private void clean() {
		freq = null;
		dataModel = null;
		fpt.clean();
	}
	
	/**
	 * Threaded projection implementation
	 * Each thread is in charge of the item at (index % threads) + threadNum
	 * of the FP-Tree header table
	 * Each thread output into its own file
	 */
	class ProjectRunnable implements Runnable {
		private int threadNum;		/* id for each thread */
		private int itemIndex;		/* header table index of item in charge of the thread */
		private OutputLayer ol;

		public ProjectRunnable(int num, String outputBase) throws Exception {
			threadNum = num;
			itemIndex = threadNum;
			ol = new OutputLayer(outputBase+"@"+Integer.toString(threadNum));
		}

		@Override
		public void run() {
			long cc = 0;
			while (itemIndex < headerTableCount) {
				if (++cc % 1000 == 0)
					log.info("Thread"+Integer.toString(threadNum)+": Projected for {} items/users ...", cc);

				HashMap<Integer, Long> counter = Maps.newHashMap();
				int item = headerTableItems[itemIndex];
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
						String out = item < other ? Integer.toString(item)
								+ SEP + other.toString() : other.toString()
								+ SEP + Integer.toString(item);
						out = out + SEP + Long.toString(pairSupport);
						ol.writeLine(out);
					}
				}
				itemIndex += numThreads;
			}
			ol.close();
		}
	}
	
	/**
	 * Run multi-threading projection
	 * 
	 * @param outputBase output path and base file name
	 */
	public void project(String outputBase) throws Exception {
		// construct FP-tree
		firstScan();
		constructTree();
		clean();	// Runtime.getRuntime().gc(); Thread.sleep(15000);
		
		// get shared header table info.
		headerTableCount = fpt.getHeaderTableCount();
		headerTableItems = fpt.getHeaderTableItems();
		
		// launch threads
		List<Thread> threads = Lists.newArrayList();
		for (int i = 0; i < numThreads; i++) {
			Runnable task = new ProjectRunnable(i, outputBase);
			Thread worker = new Thread(task);
			worker.setName(Integer.toString(i));
			worker.start();
			threads.add(worker);
		}
		
		int running;
		do {
			running = 0;
			for (Thread thread : threads) {
				if (thread.isAlive()) {
					running++;
				}
			}
		} while (running > 0);
		
		log.info("Projection finished ...");
	}

	/**
	 * Simple projection client
	 * 
	 * @param args input output minSupport numThreads
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		boolean thread = false;
		if (args.length > 3)
			thread = true;
		
		log.info("Have {} available processors ...", Runtime.getRuntime().availableProcessors());
		
		Stopwatch sw = new Stopwatch();
		sw.start();
		DataModel dataModel = new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
						new File(args[0]))));
		
		Projection proj;
		if (thread) {
			proj = new Projection(dataModel, Integer.parseInt(args[2]), Integer.parseInt(args[3]));
			proj.project(args[1]);
		} else {	// by default, single thread
			proj = new Projection(dataModel, Integer.parseInt(args[2]));
			proj.project(args[1]);
		}
		sw.stop();

		log.info("Projection process finished, used {} ms ...",
				sw.elapsed(TimeUnit.MILLISECONDS));
	}
}
