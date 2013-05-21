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


	public FPTree() {
		root = new FPTreeNode();
		headerTable = IntObjectOpenHashMap.newInstance();
	}

	public IntObjectMap<FPTreeNode[]> getHeaderTable() {
		return headerTable;
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
            curr = curr.addChild(item, headerList);
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
	private void clean() {
		FPTreeNode.children = null;
	}
}