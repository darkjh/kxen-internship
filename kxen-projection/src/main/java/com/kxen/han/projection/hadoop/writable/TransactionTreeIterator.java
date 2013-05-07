package com.kxen.han.projection.hadoop.writable;

import java.util.List;
import java.util.Stack;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Iterator that returns paths of the {@link TransactionTree} by a depth-first
 * search on the tree structure
 * 
 * @author Han JU
 *
 */
public class TransactionTreeIterator 
extends AbstractIterator<Pair<List<Long>, Long>> {
	
	private final static int INIT = -1;
	private final static int NODE = 0;
	private final static int PARENT = 1;
	
	private Stack<int[]> stack = 
			new Stack<int[]>();			/* DFS stack, {nodeId, parent} */
	private List<Integer> path;			/* path from root to the visiting node */
	private TransactionTree tree;
	
	public TransactionTreeIterator(TransactionTree tree) {
		this.tree = tree;
		stack.push(new int[]{tree.getRoot(), INIT});
		path = Lists.newArrayList();
	}
	
	/* transform the nodeID list to item ID list */
	private List<Long> prepareListToReturn() {
		List<Long> pathToReturn = Lists.newArrayList();
		for (Integer i : path) {
			pathToReturn.add(0, (long)tree.getItem(i));
		}
		return pathToReturn;
	}
	
	@Override
	protected Pair<List<Long>, Long> computeNext() {
		if (tree.isEmpty())
			return endOfData();
		
		// DFS search with a stack
		while (!stack.empty()) {
			int[] curr = stack.pop();
			int nodeId = curr[NODE];
			int parent = curr[PARENT];
			if (nodeId != tree.getRoot()) {
				path.add(0, nodeId);
			}
			
			int childCount = tree.childCount(nodeId);
			long support = tree.count(nodeId);
			if (childCount == 0) {	// end of a path
				List<Long> pathToReturn = prepareListToReturn();
				
				// keep only the common prefix of the path for next return
				int nextNodeId = -1;
				if (!stack.empty()) {
					nextNodeId = stack.peek()[PARENT];
				}
				int index = 0;
				int initSize = path.size();
				for (Integer i : path) {
					if (i == nextNodeId) {
						path = path.subList(index, path.size());
						break;
					}
					index++;
				}
				if (index == initSize) {
					path = Lists.newArrayList();
				}
				return Pair.of(pathToReturn, support);
			}
			
			// push children node onto the stack while counting the sum of
			// their support
			long suppSum = 0;
			for (int i = 0; i < childCount; i++) {
				int child = tree.childAtIndex(nodeId, i);
				suppSum += tree.count(child);
				stack.push(new int[]{child, nodeId});
			}
			
			long diff = support - suppSum;
			if (diff != 0 && parent != INIT) {	// also end of a path
				// prepare item list to return
				List<Long> pathToReturn = prepareListToReturn();
				return Pair.of(pathToReturn, diff);
			}
		}
		return endOfData();
	}
}
