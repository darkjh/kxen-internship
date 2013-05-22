package com.kxen.han.projection.fpg;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

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
	public static ObjectObjectOpenHashMap<FPTreeNode,ObjectArrayList<FPTreeNode>> children;
	
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
	
	private FPTreeNode childSearch(
			int childItem, ObjectArrayList<FPTreeNode> childList) {
		final Object[] buffer = childList.buffer;
		final int size = childList.size();
		for (int i = 0; i< size; i++) {
			FPTreeNode curr = (FPTreeNode)buffer[i];
			if (childItem == curr.getItem()) {
				return curr;
			}
		}
		return null;
	}
	
	/** add a node as a child, also add to header table */
	public FPTreeNode addChild(int childItem, FPTreeNode[] headerList) {
		return addChild(childItem, 1, headerList);
	}

	/** add a node as a child, also add to header table */
	public FPTreeNode addChild(int childItem, int inc, FPTreeNode[] headerList) { 
		ObjectArrayList<FPTreeNode> childList;
		if (children.containsKey(this)) {
			childList = children.lget();
		} else {
			childList = ObjectArrayList.newInstance();
			children.put(this, childList);
		}
		
		FPTreeNode child;
		if ((child = childSearch(childItem, childList)) != null) {
			child.incrementCount(inc);
		} else {
			child = new FPTreeNode(childItem, inc, this);
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
	
	public void incrementCount(int inc) {
		count += inc;
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
