package com.kxen.han.projection.giraph;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProjectedGraphOutputFormat
extends TextVertexOutputFormat<LongWritable, NullWritable, LongWritable> {

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
		}
		
	}
	
}
