package com.kxen.han.projection.fpg;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

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
	
	private final Long item;
	private int count;

	private FPTreeNode parent;
	private FPTreeNode next;
	private Map<Long, FPTreeNode> children;

	/** ctor for normal node */
	public FPTreeNode(long i, int c, FPTreeNode p) {
		item = i;
		count = c;
		parent = p;
	}

	/** ctor for a root node */
	public FPTreeNode() {
		item = -1L;
		count = -1;
	}
	
	/** add a node as a child, also add to header table */
	public FPTreeNode addChild(Long childItem, List<FPTreeNode> headerList) { 
		if (children == null) {
			children = Maps.newTreeMap();
		}
		
		FPTreeNode child;
		if (children.containsKey(childItem)) {
			child = children.get(childItem);
			child.incrementCount();
		} else {
			child = new FPTreeNode(childItem, 1, this);
			children.put(childItem, child);
			headerList.add(child);
		}
		return child;
	}
	
	/** add a node as a child, also add to header table */
	public FPTreeNode addChild(Long childItem, FPTreeNode[] headerList) { 
		if (children == null) {
			children = Maps.newTreeMap();
		}
		
		FPTreeNode child;
		if (children.containsKey(childItem)) {
			child = children.get(childItem);
			child.incrementCount();
		} else {
			child = new FPTreeNode(childItem, 1, this);
			children.put(childItem, child);
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

	public Long getItem() {
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
