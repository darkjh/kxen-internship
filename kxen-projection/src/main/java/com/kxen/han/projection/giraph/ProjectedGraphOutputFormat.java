package com.kxen.han.projection.giraph;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProjectedGraphOutputFormat extends
		TextVertexOutputFormat<VLongWritable, VIntWritable, VLongWritable> {

	private static final String SEP = "\t";
	private static final String SEP_TRIPLE = "\n";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ProjectedGraphVertexWriter();
	}

	class ProjectedGraphVertexWriter extends TextVertexWriterToEachLine {
		@Override
		protected Text convertVertexToLine(
				Vertex<VLongWritable, VIntWritable, VLongWritable> vertex)
				throws IOException {
			if (!ProjectionComputation.isProdNode(vertex))
				return null;
			Text out = new Text();
			String self = vertex.getId().toString();
			StringBuilder sb = new StringBuilder();
			for (Edge<VLongWritable, VLongWritable> edge : vertex.getEdges()) {
				sb.append(self).append(SEP);
				sb.append(edge.getTargetVertexId().toString()).append(SEP);
				sb.append(edge.getValue().toString()).append(SEP_TRIPLE);
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1); // delete the last \n
				out.set(sb.toString());
			}
			return out;
		}
	}
}