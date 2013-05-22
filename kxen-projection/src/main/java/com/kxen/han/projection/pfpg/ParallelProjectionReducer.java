package com.kxen.han.projection.pfpg;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntLongMap;
import com.carrotsearch.hppc.IntLongOpenHashMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import com.kxen.han.projection.fpg.FPTree;
import com.kxen.han.projection.fpg.FPTreeNode;
import com.kxen.han.projection.hadoop.writable.TransactionTree;

/**
 * FP-Tree approach projection on group-dependent transactions
 * Output frequent pair (projected graph link) of the group, possible redundancy
 * with other groups' results
 * 
 * This reducer uses the object based FP-Tree
 * 
 * @author Han JU
 *
 */
public class ParallelProjectionReducer
extends Reducer<IntWritable, TransactionTree, NullWritable, Text> {
	
	private static final Logger log = 
			LoggerFactory.getLogger(ParallelProjectionReducer.class);
	
	private int minSupport;
	private int group;
	private int numGroup;
	private int maxPerGroup;

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		minSupport = Integer.parseInt(conf.get(ParallelProjection.MIN_SUPPORT));
		numGroup = Integer.parseInt(conf.get(ParallelProjection.NUM_GROUP));
		maxPerGroup = conf.getInt(ParallelProjection.MAX_PER_GROUP, 100);
	}

	@Override
	public void reduce(IntWritable key, Iterable<TransactionTree> values,
			Context context) throws IOException, InterruptedException {
		log.info("Reduce started ...");
		// construct FP-Tree
		FPTree fpt = new FPTree();
		for (TransactionTree tt : values) {
			for (Pair<List<Long>, Long> transac : tt) {
				fpt.insertTransac(transac.getLeft(), transac.getRight().intValue());
			}
		}
		fpt.clean();
		log.info("FP-Tree construction finished, created {} nodes ...", FPTreeNode.nodeCount);

		group = key.get();

		// projection
		long cc = 0;
		IntObjectOpenHashMap<FPTreeNode[]> headerTable = fpt.getHeaderTable();
		
		// use faster iterate method
		final int[] ks = headerTable.keys;
		final Object[] vs = headerTable.values;
		final boolean[] states = headerTable.allocated;
		for (int i = 0; i < states.length; i++) {
			if (!states[i])	// not allocated
				continue;
			int item = ks[i];
			// only project for items in the current group
			// avoid lots of redundancy
//			if (ParallelProjectionMapper.getGroupByMaxPerGroup(item, maxPerGroup) != group) {
			if (ParallelProjectionMapper.getGroup(item, numGroup) != group) {
				continue;
			}
			if (++cc % 1000 == 0)
				log.info("Projected for {} items", cc);
			IntLongMap counter = new IntLongOpenHashMap();
			FPTreeNode list = ((FPTreeNode[])vs[i])[0];

			// visit all conditional path of the current item
			// merge them by counting
			FPTreeNode node = list;
			while (node != null) {
				int condSupport = node.getCount();
				FPTreeNode curr = node.getParent();
				while (!curr.isRoot()) {
					int currItem = curr.getItem();
					long count = counter.containsKey(currItem) ? counter.get(currItem) : 0l;
					counter.put(currItem, count + condSupport);
					curr = curr.getParent();
				}
				node = node.getNext();
			}
			
			// object reuse
			StringBuilder out = new StringBuilder();
			Text outText = new Text();
			
			for (IntLongCursor intLongCursor : counter) {
				long pairSupport = intLongCursor.value;
				if (pairSupport >= minSupport) {
					int k, v;
					if (item < intLongCursor.key) {
						k = item;
						v = intLongCursor.key;
					} else {
						k = intLongCursor.key;
						v = item;
					}
					out.setLength(0);	// reset string builder
					out.append(k).append("\t");
					out.append(v).append("\t");
					out.append(pairSupport);
					outText.set(out.toString());
					context.write(NullWritable.get(), outText);
				}
			}
		}
		fpt = null;
		log.info("Projection finished ...");
	}
}