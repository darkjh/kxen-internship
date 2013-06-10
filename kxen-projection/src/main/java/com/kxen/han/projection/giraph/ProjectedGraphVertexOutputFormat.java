package com.kxen.han.projection.giraph;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.kxen.han.projection.hadoop.writable.GiraphProjectionVertexValue;

/**
 * OutputFormat implementation called for write computation results
 * The output is based on each vertex, it reads the saved neighbor 
 * list and co-support values of the vertex, then construct a string
 * for output
 * 
 * @author Han JU
 *
 */
public class ProjectedGraphVertexOutputFormat 
extends TextVertexOutputFormat
<VLongWritable, GiraphProjectionVertexValue, VLongWritable> {
	
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
				Vertex<VLongWritable, GiraphProjectionVertexValue, VLongWritable> vertex)
				throws IOException {
			if (!ProjectionComputation.isProdNode(vertex))
				return null;
			Text out = new Text();
			String self = vertex.getId().toString();
			StringBuilder sb = new StringBuilder();
			int size = vertex.getValue().size;
			long[] neighbors = vertex.getValue().neighbors;
			long[] values = vertex.getValue().values;
			for (int i = 0; i < size; i++) {
				sb.append(self).append(SEP);
				sb.append(neighbors[i]).append(SEP);
				sb.append(values[i]).append(SEP_TRIPLE);
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1); // delete the last \n
				out.set(sb.toString());
			}
			return out;
		}
	}
}
