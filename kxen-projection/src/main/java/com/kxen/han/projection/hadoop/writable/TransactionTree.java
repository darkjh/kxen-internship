package com.kxen.han.projection.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Compact representation of group-dependent transactions Inspired by FP-Tree's
 * prefix tree structure, use nearly a same tree to compress group-dependent
 * transactions emitted by parallel projection mapper
 * 
 * @author Han JU
 * 
 */
public class TransactionTree 
implements Writable, Iterable<Pair<List<Long>, Long>> {
	
	private static final Logger log = LoggerFactory.getLogger(TransactionTree.class);

	/* pre-defined constants */
	private static final int ROOT_NODE_ID = 0;
	private static final int CHILDREN_INITIAL_SIZE = 2;
	private static final int INITIAL_SIZE = 8;
	private static final float GROWTH_RATE = 1.5f;

	private int[] items; 			/* node id -> item id */
	private int[] childCount; 		/* node id -> #children for that node */
	private int[][] nodeChildren; 	/* node id -> [children node ids] */
	private int[] nodeCount; 		/* count of support of each node */
	private int nodeID; 			/* next node's node id */

	private List<Pair<List<Long>, Long>> transactions;
	private boolean notCompressed;

	public TransactionTree() {
		this(INITIAL_SIZE);
	}

	public TransactionTree(int size) {
		if (size < INITIAL_SIZE) {
			size = INITIAL_SIZE;
		}
		childCount = new int[size];
		items = new int[size];
		nodeCount = new int[size];
		nodeChildren = new int[size][];
		createRootNode();
		notCompressed = false;
	}

	public TransactionTree(List<Long> transac, long support) {
		transactions = Lists.newArrayList();
		transactions.add(Pair.of(transac, support));
		notCompressed = true;
	}

	public TransactionTree(List<Pair<List<Long>, Long>> transacs) {
		transactions = transacs;
		notCompressed = true;
	}

	private final void createRootNode() {
		childCount[nodeID] = 0;
		items[nodeID] = -1;
		nodeCount[nodeID] = 0;
		if (nodeChildren[nodeID] == null) {
			nodeChildren[nodeID] = new int[CHILDREN_INITIAL_SIZE];
		}
		nodeID++;
	}

	public boolean isRoot(int nodeId) {
		return nodeId == ROOT_NODE_ID;
	}

	public int getRoot() {
		return ROOT_NODE_ID;
	}
	
	public final boolean isEmpty() {
		return nodeID <= 1;
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

	public final int getItem(int nodeId) {
		return items[nodeId];
	}
	
	public final long count(int nodeId) {
		if (nodeId >= nodeID || nodeId < 0)
			return -1;
		return nodeCount[nodeId];
	}

	public final int childAtIndex(int nodeId, int index) {
		if (childCount[nodeId] < index) {
			return -1;
		}
		return nodeChildren[nodeId][index];
	}
	
	public int childCount() {
		int sum = 0;
		for (int i = 0; i < nodeID; i++) {
			sum += childCount[i];
		}
		return sum;
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

	/** insert a transaction into the tree */
	public int insertTransac(Iterable<Long> transac, int support) {
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
				addCount(child, support);
				curr = child;
			}
		}
		return nodeCreated;
	}
	
	@Override
	public Iterator<Pair<List<Long>, Long>> iterator() {
		return notCompressed ? 
				transactions.iterator() : new TransactionTreeIterator2(this);
	}

	private final int createNode(int parentNodeId, int item) {
		return createNode(parentNodeId, item, 1);
	}
	
	private final int createNode(int parentNodeId, int item, int support) {
		if (nodeID >= this.items.length) {
			resize();
		}

		childCount[nodeID] = 0;
		this.items[nodeID] = item;
		nodeCount[nodeID] = support;

		if (nodeChildren[nodeID] == null) {
			nodeChildren[nodeID] = new int[CHILDREN_INITIAL_SIZE];
		}

		int childNodeId = nodeID++;
		addChild(parentNodeId, childNodeId);
		return childNodeId;
	}
	
	public TransactionTree getCompressedTree() {
		TransactionTree ctree = new TransactionTree();
		int node = 0;
		int size = 0;
		List<Pair<List<Long>, Long>> transacs = Lists.newArrayList();
		for (Pair<List<Long>, Long> p : this) {
			transacs.add(p);
			node += ctree.insertTransac(p.getLeft(), p.getRight().intValue());
			size += p.getLeft().size() + 2;
		}

//		log.info("----------------------------");
//	        log.info("Nodes in UnCompressed Tree: {} ", nodeID);
//	        log.info("UnCompressed Tree Size: {}", (this.nodeID * 4 * 4 + this.childCount() * 4) / 1000000.0);
//	        log.info("Nodes in Compressed Tree: {} ", node);
//	        log.info("Compressed Tree Size: {}", (node * 4 * 4 + ctree.childCount() * 4) / 1000000.0);
//	        log.info("TransactionSet Size: {}", size * 16 / 1000000.0);
//	    log.info("----------------------------");
		
		// compare sizes then decide which format to use
		// Long object takes 24 bytes (8 header, 8 long, some other info. + padding)
		// int takes 4 bytes
		// TODO for the moment combiner does not work well (it actually increase mapper output size)
		// need a more precise calculation of the ouput size
		if (node * 4 * 4 + ctree.childCount() * 4 <= size * 8) {
			log.info("Compressed FP-Tree");
			return ctree;
		} else {
			log.info("Uncompressed list");
			return new TransactionTree(transacs);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		notCompressed = in.readBoolean();

		VIntWritable intWriter = new VIntWritable();
		VLongWritable longWriter = new VLongWritable();

		if (notCompressed) {
			intWriter.readFields(in);
			int size = intWriter.get();
			transactions = Lists.newArrayListWithCapacity(size);
			for (int i = 0; i < size; i++) {
				intWriter.readFields(in);
				int length = intWriter.get();
				List<Long> items = Lists.newArrayList();
				for (int j = 0; j < length; j++) {
					longWriter.readFields(in);
					items.add(longWriter.get());
				}
				longWriter.readFields(in);
				Long support = longWriter.get();
				transactions.add(Pair.of(items, support));
			}
		} else {
			intWriter.readFields(in);
			nodeID = intWriter.get();
			items = new int[nodeID];
			nodeCount = new int[nodeID];
			childCount = new int[nodeID];
			nodeChildren = new int[nodeID][];
			for (int i = 0; i < nodeID; i++) {
				intWriter.readFields(in);
				items[i] = intWriter.get();
				intWriter.readFields(in);
				nodeCount[i] = intWriter.get();
				intWriter.readFields(in);
				int childCountAtI = intWriter.get();
				childCount[i] = childCountAtI;
				nodeChildren[i] = new int[childCountAtI];
				for (int j = 0; j < childCountAtI; j++) {
					intWriter.readFields(in);
					nodeChildren[i][j] = intWriter.get();
				}
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(notCompressed);
		VIntWritable intWriter = new VIntWritable();
		VLongWritable longWriter = new VLongWritable();
		
		if (notCompressed) {
			intWriter.set(transactions.size());
			intWriter.write(out);
			for (Pair<List<Long>, Long> transac : transactions) {
				intWriter.set(transac.getLeft().size());
				intWriter.write(out);
				List<Long> items = transac.getLeft();
				for (int i = 0; i < items.size(); i++) {
					long item = items.get(i);
					longWriter.set(item);
					longWriter.write(out);
				}
				longWriter.set(transac.getRight());
				longWriter.write(out);
			}
		} else {
			intWriter.set(nodeID);
			intWriter.write(out);
			for (int i = 0; i < nodeID; i++) {
				intWriter.set(items[i]);
				intWriter.write(out);
				longWriter.set(nodeCount[i]);
				longWriter.write(out);
				intWriter.set(childCount[i]);
				intWriter.write(out);
				int max = childCount[i];
				for (int j = 0; j < max; j++) {
					intWriter.set(nodeChildren[i][j]);
					intWriter.write(out);
				}
			}
		}
	}

	private void resize() {
		int size = (int) (GROWTH_RATE * nodeID);
		if (size < INITIAL_SIZE) {
			size = INITIAL_SIZE;
		}

		int[] oldChildCount = childCount;
		int[] oldItems = items;
		int[] oldNodeCount = nodeCount;
		int[][] oldNodeChildren = nodeChildren;

		childCount = new int[size];
		items = new int[size];
		nodeCount = new int[size];

		nodeChildren = new int[size][];

		System.arraycopy(oldChildCount, 0, this.childCount, 0, nodeID);
		System.arraycopy(oldItems, 0, this.items, 0, nodeID);
		System.arraycopy(oldNodeCount, 0, this.nodeCount, 0, nodeID);
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