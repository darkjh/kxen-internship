package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

public class ProjectionVertex 
extends Vertex<VLongWritable,NullWritable,VLongWritable,TransactionWritable> {
	
	public static String MIN_SUPPORT = "minSupport";
	
	private List<TransactionWritable> neighborList;
	private boolean isProdNode;
	
	public boolean isProdNode() {
		return isProdNode;
	}
	
	@Override
	public void compute(Iterable<TransactionWritable> messages)
			throws IOException {
		// by construction a product node has no outgoing edge
		isProdNode = getNumEdges() == 0;
		// step 0, user node sends its neighbor list
		if (getSuperstep() == 0 && !isProdNode) {
			List<Long> neighbors = Lists.newArrayList();
			
			for (Edge<VLongWritable,VLongWritable> edge : getEdges()) {
				neighbors.add(edge.getTargetVertexId().get());
			}
			for (Edge<VLongWritable,VLongWritable> edge : getEdges()) {
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
			removeVertexRequest(getId());
			
			// user nodes stop
			voteToHalt();
		}
		
		// save msgs
		if (getSuperstep() == 1) {
			neighborList = Lists.newArrayList();
			for (TransactionWritable msg : messages) {
				neighborList.add(new TransactionWritable(msg, msg.size()));
			}
		}
		
		// calculation
		if (getSuperstep() >= 1 && getSuperstep()-1 == getId().get()%10) {
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
					addEdge(EdgeFactory.create(k, v));
				}
			}
			voteToHalt();
		}
	}
}