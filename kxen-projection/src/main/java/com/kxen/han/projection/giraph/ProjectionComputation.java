package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;

import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import com.kxen.han.projection.hadoop.writable.GiraphProjectionVertexValue;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

public class ProjectionComputation 
extends BasicComputation
<VLongWritable,GiraphProjectionVertexValue,VLongWritable,TransactionWritable> {
	
	public static String MIN_SUPPORT = GiraphProjection.SUPP;
	public static String ROUND = GiraphProjection.GROUP;
	
	private static Map<Long,Long> FREQ;
	private static int TOTAL_ROUND;
	private static int MIN_SUPP;
	
	/** by construction a user node's id is negative */
	public static boolean isProdNode(Vertex<VLongWritable,?,?> v) {
		return v.getId().get() > 0;
	}

	@SuppressWarnings("unchecked")
	private void initFreq() throws IOException {
		GiraphConfiguration conf = getConf();
		if (null == FREQ) {
			System.out.println("Init freq list ...");
			Path[] caches = DistributedCache.getLocalCacheFiles(conf);
			FileSystem fs = FileSystem.getLocal(conf); // cache is stored locally
			Path fListPath = fs.makeQualified(caches[0]);
			ObjectInputStream ois = new ObjectInputStream(fs.open(fListPath));
			try {
				FREQ = (Map<Long, Long>) ois.readObject();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				ois.close();
			}
		}
	}
	
	/** init the support filter map and some static variable */
	private void init() throws IOException {
		initFreq();
		MIN_SUPP = getConf().getInt(MIN_SUPPORT, 2);
		TOTAL_ROUND = getConf().getInt(ROUND, 20);
	}
	
	@Override
	public void compute(
			Vertex<VLongWritable,GiraphProjectionVertexValue,VLongWritable> vertex,
			Iterable<TransactionWritable> messages)
			throws IOException {
		// init f-list for filtering
		if (getSuperstep() == 0)
			init();
		
		// init user node's data
		if (getSuperstep() == 0 && !isProdNode(vertex)) {
			int len = vertex.getNumEdges();
			long[] neighbors = new long[len];
			int i = 0;
			for (Edge<VLongWritable,VLongWritable> edge : vertex.getEdges()) {
				neighbors[i] = edge.getTargetVertexId().get();
				i++;
			}
			Arrays.sort(neighbors);
			vertex.getValue().round = TOTAL_ROUND;
			vertex.getValue().neighbors = neighbors;
		}
		
		// if a user node, dispatch its neighbor list
		if (!isProdNode(vertex)) {
			int len = vertex.getNumEdges();
			long[] neighbors = vertex.getValue().neighbors;
			
			// send messages
			// stop at (length-1), avoid sending empty message
			VLongWritable target = new VLongWritable();
			for (int i = 0; i < len-1; i++) {
				target.set(neighbors[i]);
				if ((target.get() % TOTAL_ROUND) != getSuperstep())
					continue;
				if (!FREQ.containsKey(target.get()))
					continue;
				
				sendMessage(target, 
						new TransactionWritable(neighbors, i+1, len-i-1));
			}

			int remaining = vertex.getValue().round - 1;
			if (remaining == 0) {
				removeVertexRequest(vertex.getId());
			} else if (remaining == -1) {
				// stop user node one superstep later
				// because removeVertexRequest needs one step to execute
				vertex.voteToHalt();
			} else {
				vertex.getValue().round = remaining;
			}
		}

		// calculation in product node
		if (isProdNode(vertex)) {
			LongLongOpenHashMap counter = LongLongOpenHashMap.newInstance();
			for (TransactionWritable transac : messages) {
				for (Long item : transac) {
					long count = counter.containsKey(item) ?
							counter.get(item) : 0l;
					counter.put(item, count+1l);
				}
			}
			VLongWritable k = new VLongWritable();
			VLongWritable v = new VLongWritable();
			for (LongLongCursor cursor : counter) {
				if (cursor.value >= MIN_SUPP) {
					k.set(cursor.key);
					v.set(cursor.value);
					vertex.addEdge(EdgeFactory.create(k, v));
				}
			}
			// use giraph.doOutputDuringComputation
			removeVertexRequest(vertex.getId());
			vertex.voteToHalt();
		}
	}
}