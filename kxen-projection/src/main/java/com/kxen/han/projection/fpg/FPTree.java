package com.kxen.han.projection.fpg;

import java.util.List;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

/**
 * A FP-Tree implementation
 *
 * Based on article: www.cs.uiuc.edu/~hanj/pdf/sigmod00.pdf
 *
 * @author Han JU
 *
 */
public class FPTree {
	private FPTreeNode root;
	private IntObjectOpenHashMap<FPTreeNode[]> headerTable;
	
	public FPTree() {
		root = new FPTreeNode();
		headerTable = IntObjectOpenHashMap.newInstance();
		FPTreeNode.children = ObjectObjectOpenHashMap.newInstance();
		FPTreeNode.nodeCount = 0l;
	}

	public IntObjectOpenHashMap<FPTreeNode[]> getHeaderTable() {
		return headerTable;
	}
	
	public void insertTransac(Iterable<Long> transac) {
		insertTransac(transac, 1);
	}

	public void insertTransac(Iterable<Long> transac, int support) {
        FPTreeNode curr = root;
        for (Long longItem : transac) {
            int item = longItem.intValue();
            FPTreeNode[] headerList;
            if (headerTable.containsKey(item)) {
                headerList = headerTable.lget();
            } else {
                headerList = new FPTreeNode[]{null, null};
                headerTable.put(item, headerList);
            }
            curr = curr.addChild(item, support, headerList);
        }
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

	/** help GC, children information is of no use in projection */
	public void clean() {
		FPTreeNode.children = null;
	}
}