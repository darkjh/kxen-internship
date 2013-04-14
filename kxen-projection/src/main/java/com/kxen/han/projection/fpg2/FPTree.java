package com.kxen.han.projection.fpg2;

import java.util.Arrays;
import java.util.List;

/**
 * FP-tree implementation uses only primitive arrays, memory efficient
 * Adapted from Apache Mahout source code:
 * 	org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPTree
 * 
 * Original @author Robin Anil
 * @author Han JU
 */
public class FPTree {
	/* pre-defined constants */
	private static final int ROOT_NODE_ID = 0;
	private static final int CHILDREN_INITIAL_SIZE = 2;
	private static final int HEADER_TABLE_INITIAL_SIZE = 4;
	private static final int INITIAL_SIZE = 8;
	private static final float GROWTH_RATE = 1.5f;
	private static final int HEADER_TABLE_BLOCK_SIZE = 2;
	private static final int HT_LAST = 1;
	private static final int HT_NEXT = 0;
	
	private int[] items;					/* node id -> item id */
	private int[] childCount;				/* node id -> #children for that node */
	private long[] headerTableItemCount;	/* ht index -> #item nodes in tree */
	private int[] headerTableItems;			/* ht index -> item id */
	private int headerTableCount;			/* #frequent items */
	private int[] headerTableLookup;		/* item id -> ht index */
	private int[][] headerTable;			/* ht index ->  [same item links] */
	private int[] next;						/* node id -> node id of next same item in tree */
	private int[][] nodeChildren;			/* node id -> [children node ids] */
	private int[] nodeCount;				/* count of support of each node */
	private int nodeID;						/* next node's node id */
	private int[] parent;					/* node id -> parent node id */

	public FPTree() {
		this(INITIAL_SIZE);
	}
	
	/** 
	 * Init the tree structre with a given size
	 * @param size
	 */
	public FPTree(int size) {
		if (size < INITIAL_SIZE) {
			size = INITIAL_SIZE;
		}

		parent = new int[size];
		next = new int[size];
		childCount = new int[size];
		items = new int[size];
		nodeCount = new int[size];
		nodeChildren = new int[size][];

		headerTableItems = new int[HEADER_TABLE_INITIAL_SIZE];
		headerTableItemCount = new long[HEADER_TABLE_INITIAL_SIZE];
		headerTableLookup = new int[HEADER_TABLE_INITIAL_SIZE];
		Arrays.fill(headerTableLookup, -1);
		headerTable = new int[HEADER_TABLE_INITIAL_SIZE][];

		createRootNode();
	}
	
	public boolean isRoot(int nodeId) {
		return nodeId == ROOT_NODE_ID;
	}
	
	public int getRoot() {
		return ROOT_NODE_ID;
	}

	public final void addChild(int parentNodeId, int childNodeId) {
		int length = childCount[parentNodeId];
		if (length >= nodeChildren[parentNodeId].length) {
			resizeChildren(parentNodeId);
		}
		nodeChildren[parentNodeId][length++] = childNodeId;
		childCount[parentNodeId] = length;

	}

	public final void addCount(int nodeId, int count) {
		if (nodeId < nodeID) {
			this.nodeCount[nodeId] += count;
		}
	}

	public final void addHeaderNext(int item, int nodeId) {
		int index = getHeaderIndex(item);
		if (headerTable[index][HT_NEXT] == -1) {
			headerTable[index][HT_NEXT] = nodeId;
			headerTable[index][HT_LAST] = nodeId;
		} else {
			setNext(headerTable[index][HT_LAST], nodeId);
			headerTable[index][HT_LAST] = nodeId;
		}
	}

	public final int getItem(int nodeId) {
		return items[nodeId];
	}

	public final int childAtIndex(int nodeId, int index) {
		if (childCount[nodeId] < index) {
			return -1;
		}
		return nodeChildren[nodeId][index];
	}

	public final int childCount(int nodeId) {
		return childCount[nodeId];
	}
	
	/** linear search for a child item */
	public final int childWithItem(int nodeId, int childItem) {
		int length = childCount[nodeId];
		for (int i = 0; i < length; i++) {
			if (items[nodeChildren[nodeId][i]] == childItem) {
				return nodeChildren[nodeId][i];
			}
		}
		return -1;
	}
	
	public final void reset() {
		nodeID = 0;
		headerTableCount = 0;
		Arrays.fill(headerTableLookup, -1);
		createRootNode();
	}

	public final long count(int nodeId) {
		return nodeCount[nodeId];
	}
	
	/** insert a transaction into the tree */
	public int insertTransac(List<Long> transac) {
		int curr = getRoot();
		int nodeCreated = 0;

		for (long longItem : transac) {
			int item = (int) longItem;
			int child = childWithItem(curr, item);
			if (child == -1) {
				child = createNode(curr, item);
				curr = child;
				nodeCreated++;
			} else {
				addCount(child, 1);
				curr = child;
			}
		}
		return nodeCreated;
	}

	private final int createNode(int parentNodeId, int item) {
		if (nodeID >= this.items.length) {
			resize();
		}

		childCount[nodeID] = 0;
		next[nodeID] = -1;
		parent[nodeID] = parentNodeId;
		this.items[nodeID] = item;
		nodeCount[nodeID] = 1;

		if (nodeChildren[nodeID] == null) {
			nodeChildren[nodeID] = new int[CHILDREN_INITIAL_SIZE];
		}

		int childNodeId = nodeID++;
		addChild(parentNodeId, childNodeId);
		addHeaderNext(item, childNodeId);
		return childNodeId;
	}

	private final void createRootNode() {
		childCount[nodeID] = 0;
		next[nodeID] = -1;
		parent[nodeID] = 0;
		items[nodeID] = -1;
		nodeCount[nodeID] = 0;
		if (nodeChildren[nodeID] == null) {
			nodeChildren[nodeID] = new int[CHILDREN_INITIAL_SIZE];
		}
		nodeID++;
	}

	public final int getItemAtHeaderIndex(int index) {
		return headerTableItems[index];
	}

	public final int getHeaderNext(int item) {
		int index = getHeaderIndex(item);
		return headerTable[index][HT_NEXT];
	}

	public final long getHeaderSupportCount(int item) {
		int index = getHeaderIndex(item);
		return headerTableItemCount[index];
	}

	public final int[] getHeaderTableItems() {
		int[] items = new int[headerTableCount];
		System.arraycopy(headerTableItems, 0, items, 0,
				headerTableCount);
		return items;
	}

	public final int getHeaderTableCount() {
		return headerTableCount;
	}

	public final boolean isEmpty() {
		return nodeID <= 1;
	}

	public final int getNext(int nodeId) {
		return next[nodeId];
	}

	public final int getParent(int nodeId) {
		return parent[nodeId];
	}

	public final void setNext(int nodeId, int nextNode) {
		if (nodeId < nodeID) {
			this.next[nodeId] = nextNode;
		}
	}

	public final void setParent(int nodeId, int parentNode) {
		if (nodeId < nodeID) {
			this.parent[nodeId] = parentNode;

			int length = childCount[parentNode];
			if (length >= nodeChildren[parentNode].length) {
				resizeChildren(parentNode);
			}
			nodeChildren[parentNode][length++] = nodeId;
			childCount[parentNode] = length;
		}
	}
	
	/* called after construction of the tree */
	public void clean() {
		// children info has no use in projection
		childCount = null;
		nodeChildren = null;
	}
	
	// seems to assume item is continuous
	private int getHeaderIndex(int item) {
		if (item >= headerTableLookup.length) {
			resizeHeaderLookup(item);
		}
		int index = headerTableLookup[item];
		if (index == -1) { // if item didn't exist;
			if (headerTableCount >= headerTableItems.length) {
				resizeHeaderTable();
			}
			headerTableItems[headerTableCount] = item;
			if (headerTable[headerTableCount] == null) {
				headerTable[headerTableCount] = new int[HEADER_TABLE_BLOCK_SIZE];
			}
			headerTableItemCount[headerTableCount] = 0;
			headerTable[headerTableCount][HT_NEXT] = -1;
			headerTable[headerTableCount][HT_LAST] = -1;
			index = headerTableCount++;
			headerTableLookup[item] = index;
		}
		return index;
	}

	private void resize() {
		int size = (int) (GROWTH_RATE * nodeID);
		if (size < INITIAL_SIZE) {
			size = INITIAL_SIZE;
		}

		int[] oldChildCount = childCount;
		int[] oldItems = items;
		int[] oldNodeCount = nodeCount;
		int[] oldParent = parent;
		int[] oldNext = next;
		int[][] oldNodeChildren = nodeChildren;

		childCount = new int[size];
		items = new int[size];
		nodeCount = new int[size];
		parent = new int[size];
		next = new int[size];

		nodeChildren = new int[size][];

		System.arraycopy(oldChildCount, 0, this.childCount, 0, nodeID);
		System.arraycopy(oldItems, 0, this.items, 0, nodeID);
		System.arraycopy(oldNodeCount, 0, this.nodeCount, 0, nodeID);
		System.arraycopy(oldParent, 0, this.parent, 0, nodeID);
		System.arraycopy(oldNext, 0, this.next, 0, nodeID);
		System.arraycopy(oldNodeChildren, 0, this.nodeChildren, 0, nodeID);
	}

	private void resizeChildren(int nodeId) {
		int length = childCount[nodeId];
		int size = (int) (GROWTH_RATE * length);
		if (size < CHILDREN_INITIAL_SIZE) {
			size = CHILDREN_INITIAL_SIZE;
		}
		int[] oldNodeChildren = nodeChildren[nodeId];
		nodeChildren[nodeId] = new int[size];
		System.arraycopy(oldNodeChildren, 0, this.nodeChildren[nodeId], 0,
				length);
	}

	private void resizeHeaderLookup(int item) {
		int size = (int) (item * GROWTH_RATE);
		int[] oldLookup = headerTableLookup;
		headerTableLookup = new int[size];
		Arrays.fill(headerTableLookup, oldLookup.length, size, -1);
		System.arraycopy(oldLookup, 0, this.headerTableLookup, 0,
				oldLookup.length);
	}

	private void resizeHeaderTable() {
		int size = (int) (GROWTH_RATE * headerTableCount);
		if (size < HEADER_TABLE_INITIAL_SIZE) {
			size = HEADER_TABLE_INITIAL_SIZE;
		}

		int[] oldItems = headerTableItems;
		long[] oldItemCount = headerTableItemCount;
		int[][] oldTable = headerTable;
		headerTableItems = new int[size];
		headerTableItemCount = new long[size];
		headerTable = new int[size][];
		System.arraycopy(oldItems, 0, this.headerTableItems, 0,
				headerTableCount);
		System.arraycopy(oldItemCount, 0, this.headerTableItemCount,
				0, headerTableCount);
		System.arraycopy(oldTable, 0, this.headerTable, 0,
				headerTableCount);
	}

	private void toStringHelper(StringBuilder sb, int currNode, String prefix) {
		if (childCount[currNode] == 0) {
			sb.append(prefix).append("-{item:").append(items[currNode])
					.append(", id: ").append(currNode).append(", cnt:")
					.append(nodeCount[currNode]).append("}\n");
		} else {
			StringBuilder newPre = new StringBuilder(prefix);
			newPre.append("-{item:").append(items[currNode])
					.append(", id: ").append(currNode).append(", cnt:")
					.append(nodeCount[currNode]).append('}');
			StringBuilder fakePre = new StringBuilder();
			while (fakePre.length() < newPre.length()) {
				fakePre.append(' ');
			}
			for (int i = 0; i < childCount[currNode]; i++) {
				toStringHelper(sb, nodeChildren[currNode][i], (i == 0 ? newPre
						: fakePre).toString() + '-' + i + "->");
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[FPTree\n");
		toStringHelper(sb, 0, "  ");
		sb.append("\n]\n");
		return sb.toString();
	}
}
