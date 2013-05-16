package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.Lists;
import com.kxen.han.projection.hadoop.writable.TransactionWritable;

public class ProjectionVertex 
extends Vertex<LongWritable,NullWritable,LongWritable,TransactionWritable> {
	
	/** by construction a product node has no outgoing edge */
	public boolean isProdNode() {
		return this.getNumEdges() == 0;
	}
	
	@Override
	public void compute(Iterable<TransactionWritable> messages)
			throws IOException {
		// step 0, user node sends its neighbor list
		if (getSuperstep() == 0 && !isProdNode()) {
			List<Long> neighbors = Lists.newArrayList();
			
			for (Edge<LongWritable,LongWritable> edge : getEdges()) {
				neighbors.add(edge.getTargetVertexId().get());
			}
			
			for (Edge<LongWritable,LongWritable> edge : getEdges()) {
				Long target = edge.getTargetVertexId().get();
				List<Long> msg = Lists.newArrayList();
				for (Long item : neighbors) {
					if (target < item) {
						msg.add(item);
					}
				}
				sendMessage(edge.getTargetVertexId(), 
						new TransactionWritable(msg));
			}
			
			// then disconnect user nodes from the graph
			LongWritable self = this.getId();
			for (Edge<LongWritable,LongWritable> edge : getEdges()) {
				removeEdgesRequest(self, edge.getTargetVertexId());
			}
		}
	}
}