package com.kxen.han.projection.fpg;

import java.util.List;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Lists;

/**
 * FP-Tree node implementation
 * 
 * Based on article: www.cs.uiuc.edu/~hanj/pdf/sigmod00.pdf
 * 
 * @author Han JU
 *
 */
public class FPTreeNode {
	// keep track of #node created
	public static long nodeCount;
	public static ObjectObjectOpenHashMap<FPTreeNode,List<FPTreeNode>> children = 
			ObjectObjectOpenHashMap.newInstance();
	
	private final int item;
	private int count;

	private FPTreeNode parent;
	private FPTreeNode next;

	/** ctor for normal node */
	public FPTreeNode(long i, int c, FPTreeNode p) {
		item = (int) i;
		count = c;
		parent = p;
	}

	/** ctor for a root node */
	public FPTreeNode() {
		item = -1;
		count = -1;
	}
	
	private FPTreeNode childSearch(int childItem, List<FPTreeNode> childList) {
		for (FPTreeNode node : childList) {
			if (node.getItem() == childItem)
				return node;
		}
		return null;
	}

	/** add a node as a child, also add to header table */
	public FPTreeNode addChild(int childItem, FPTreeNode[] headerList) { 
		List<FPTreeNode> childList;
		if (children.containsKey(this)) {
			childList = children.lget();
		} else {
			childList = Lists.newArrayList();
			children.put(this, childList);
		}
		
		FPTreeNode child;
		if ((child = childSearch(childItem, childList)) != null) {
			child.incrementCount();
		} else {
			child = new FPTreeNode(childItem, 1, this);
			children.get(this).add(child);
			nodeCount++;
			
			if (headerList[0] == null) {
				// first occurrence of this item
				headerList[0] = child;
				headerList[1] = child;
			} else {
				headerList[1].setNext(child);
				headerList[1] = child; 
			}
		}
		return child;
	}
	
	public boolean isRoot() {
		return item == -1;
	}
	
	public FPTreeNode getParent() {
		return parent;
	}

	public int getItem() {
		return item;
	}

	public int getCount() {
		return count;
	}
	
	public int incrementCount() {
		return ++count;
	}
	
	public FPTreeNode getNext() {
		return next;
	}

	public void setNext(FPTreeNode next) {
		this.next = next;
	}
	
	@Override
	public String toString() {
		return Long.toString(item)+":"+Integer.toString(count);
	}
}
