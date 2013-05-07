package com.kxen.han.projection.hadoop.writable;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Generates a List of transactions view of Transaction Tree by doing Depth
 * First Traversal on the tree structure
 */
final class TransactionTreeIterator2 extends
		AbstractIterator<Pair<List<Long>, Long>> {

	private final Stack<int[]> depth = new Stack<int[]>();
	private final TransactionTree transactionTree;

	TransactionTreeIterator2(TransactionTree transactionTree) {
		this.transactionTree = transactionTree;
		depth.push(new int[] { 0, -1 });
	}

	@Override
	protected Pair<List<Long>, Long> computeNext() {
		if (depth.isEmpty()) {
			return endOfData();
		}

		long sum;
		int childId;
		do {
			int[] top = depth.peek();
			while (top[1] + 1 == transactionTree.childCount(top[0])) {
				depth.pop();
				top = depth.peek();
			}
			if (depth.isEmpty()) {
				return endOfData();
			}
			top[1]++;
			childId = transactionTree.childAtIndex(top[0], top[1]);
			depth.push(new int[] { childId, -1 });

			sum = 0;
			for (int i = transactionTree.childCount(childId) - 1; i >= 0; i--) {
				sum += transactionTree.count(transactionTree.childAtIndex(
						childId, i));
			}
		} while (sum == transactionTree.count(childId));

		List<Long> data = Lists.newArrayList();
		Iterator<int[]> it = depth.iterator();
		it.next();
		while (it.hasNext()) {
			data.add((long)transactionTree.getItem(it.next()[0]));
		}

		Pair<List<Long>, Long> returnable = Pair.of(
				data, transactionTree.count(childId) - sum);

		int[] top = depth.peek();
		while (top[1] + 1 == transactionTree.childCount(top[0])) {
			depth.pop();
			if (depth.isEmpty()) {
				break;
			}
			top = depth.peek();
		}
		return returnable;
	}
}