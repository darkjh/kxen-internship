package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

public class ProjectionComputation 
extends BasicComputation<VLongWritable,TransactionWritable,VLongWritable,TransactionWritable> {
	
	public static String MIN_SUPPORT = "minSupport";
	
	/** by construction a product node has no out edge initially */
	public boolean isProdNode(Vertex<?,?,?> v) {
		return v.getNumEdges() == 0;
	}
	
	@Override
	public void compute(Vertex<VLongWritable, TransactionWritable, VLongWritable> vertex, 
			Iterable<TransactionWritable> messages)
			throws IOException {
		// step 0, user node sends its neighbor list
		if (getSuperstep() == 0 && !isProdNode(vertex)) {
			List<Long> neighbors = Lists.newArrayList();
			
			for (Edge<VLongWritable,VLongWritable> edge : vertex.getEdges()) {
				neighbors.add(edge.getTargetVertexId().get());
			}
			for (Edge<VLongWritable,VLongWritable> edge : vertex.getEdges()) {
				Long target = edge.getTargetVertexId().get();
				List<Long> msg = Lists.newArrayList();
				for (Long item : neighbors) {
					if (target < item) {
						msg.add(item);
					}
				}
				if (!msg.isEmpty()) {
					sendMessage(edge.getTargetVertexId(), 
							new TransactionWritable(msg));
				}
			}
			
			// then remove user nodes from the graph
			removeVertexRequest(vertex.getId());
			
			// user nodes stop
			vertex.voteToHalt();
		}
		
		// save msgs
		if (getSuperstep() == 1) {
			neighbor = Lists.newArrayList();
			for (TransactionWritable msg : messages) {
				neighborList.add(new TransactionWritable(msg, msg.size()));
			}
		}
		
		// calculation
		if (getSuperstep() >= 1 && getSuperstep()-1 == vertex.getId().get()%10) {
			Map<Long, Long> counter = Maps.newHashMap();
			for (TransactionWritable transac : neighborList) {
				for (Long item : transac) {
					Long count = counter.containsKey(item) ?
							counter.get(item) : 0l;
					counter.put(item, count+1l);
				}
			}
			neighborList = null;
			VLongWritable k = new VLongWritable();
			VLongWritable v = new VLongWritable();
			int minSupport = getConf().getInt(MIN_SUPPORT, 2);
			for (Entry<Long, Long> entry : counter.entrySet()) {
				if (entry.getValue() >= minSupport) {
					k.set(entry.getKey());
					v.set(entry.getValue());
					vertex.addEdge(EdgeFactory.create(k, v));
				}
			}
			vertex.voteToHalt();
		}
	}
}