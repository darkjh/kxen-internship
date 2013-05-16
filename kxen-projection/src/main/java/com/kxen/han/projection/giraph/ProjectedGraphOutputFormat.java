package com.kxen.han.projection.giraph;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProjectedGraphOutputFormat
extends TextVertexOutputFormat<LongWritable, NullWritable, LongWritable> {

	private static final String SEP = "\t";
	private static final String SEP_ITEM = " ";
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return null;
	}
	
	class ProjectedGraphVertexWriter
	extends TextVertexWriterToEachLine {
		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, NullWritable, LongWritable, ?> vertex)
				throws IOException {
			Text out = new Text();
			if (vertex.getNumEdges() != 0) { // only item nodes
				StringBuilder sb = new StringBuilder(vertex.getId().toString());
				sb.append(SEP);
				for (Edge<LongWritable,LongWritable> edge : vertex.getEdges()) {
					sb.append(edge.getTargetVertexId().toString());
					sb.append(SEP_ITEM);
				}
				sb.deleteCharAt(sb.length()-1); // delete last space
				out.set(sb.toString());
			}
			return out;
		}
	}
}
