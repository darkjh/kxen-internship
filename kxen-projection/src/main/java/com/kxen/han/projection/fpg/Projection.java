package com.kxen.han.projection.fpg;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

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
	
	public static void project(String file, boolean transpose, 
			OutputLayer ol, int minSupport) throws Exception {
		DataModel model = new GenericBooleanPrefDataModel( 
				GenericBooleanPrefDataModel.toDataMap(
						new FileDataModel(new File(file), transpose, Long.MAX_VALUE)));
		project(model, ol, minSupport);
	}
	
	public static void project(String file, OutputLayer ol, 
			int minSupport) throws Exception {
		DataModel model = new GenericBooleanPrefDataModel( 
				GenericBooleanPrefDataModel.toDataMap(
						new FileDataModel(new File(file))));
		project(model, ol, minSupport);
	}
	
	public static void project(DataModel model, OutputLayer ol, 
			int minSupport) throws Exception {
		FPTree fpt = new FPTree(model, minSupport);
		Map<Integer, FPTreeNode[]> headerTable = fpt.getHeaderTable();
		long cc = 0;
		
		for (Integer item : headerTable.keySet()) {
			if (++cc % 2500 == 0)
				log.info("Projected for {} items/users ...", cc);
			HashMap<Integer, Integer> counter = Maps.newHashMap();
			FPTreeNode list = headerTable.get(item)[0];
			
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
					String out = item < i ? 
						item.toString() + SEP + i.toString()
							: i.toString() + SEP + item.toString();
					out = out + SEP + Integer.toString(pairSupport);
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
		Projection.project(args[0], new OutputLayer(args[1]), 
				Integer.parseInt(args[2]));
		sw.stop();
		
		log.info("Projection process finished, used {} ms ...", 
				sw.elapsed(TimeUnit.MILLISECONDS));
	}
}
