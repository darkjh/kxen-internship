package com.kxen.han.projection.giraph.sssp;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SimpleShortestPathVertex extends
		Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	public static class SimpleShortestPathsVertexInputFormat extends
	TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

		@Override
		public TextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException {
			
			return null;
		}
		
	}


	/** Source id. */
	public static final String SOURCE_ID = "giraph.shortestPathsBenchmark.sourceId";
	/** Default source id. */
	public static final long SOURCE_ID_DEFAULT = 1;

	private boolean isSource() {
		return getId().get() == getConf().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
	}

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			setValue(new DoubleWritable(Double.MAX_VALUE));
		}

		double minDist = isSource() ? 0d : Double.MAX_VALUE;
		for (DoubleWritable message : messages) {
			minDist = Math.min(minDist, message.get());
		}

		if (minDist < getValue().get()) {
			setValue(new DoubleWritable(minDist));
			for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
				double distance = minDist + edge.getValue().get();
				sendMessage(edge.getTargetVertexId(), new DoubleWritable(
						distance));
			}
		}

		voteToHalt();
	}
}
