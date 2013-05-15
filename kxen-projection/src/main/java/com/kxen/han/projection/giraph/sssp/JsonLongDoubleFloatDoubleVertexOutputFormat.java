package com.kxen.han.projection.giraph.sssp;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

public class JsonLongDoubleFloatDoubleVertexOutputFormat 
extends	TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new JsonLongDoubleFloatDoubleVertexWriter();
	}

	/**
	 * VertexWriter that supports vertices with <code>double</code> values
	 * and <code>float</code> out-edge weights.
	 */
	private class JsonLongDoubleFloatDoubleVertexWriter extends
			TextVertexWriterToEachLine {
		@Override
		public Text convertVertexToLine(
				Vertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex)
						throws IOException {
			JSONArray jsonVertex = new JSONArray();
			try {
				jsonVertex.put(vertex.getId().get());
				jsonVertex.put(vertex.getValue().get());
			} catch (JSONException e) {
				throw new IllegalArgumentException(
						"writeVertex: Couldn't write vertex " + vertex);
			}
			return new Text(jsonVertex.toString());
		}
	}
}
