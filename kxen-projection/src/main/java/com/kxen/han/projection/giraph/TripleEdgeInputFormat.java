package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat implementation for Giraph based projection
 * It reads an edge format input
 *   user id <TAB> item id <TAB> others ...
 * 
 * For each line, it construct an edge from user to item
 * User vertex id during the program is NEGATIVE, thus avoid vertex id 
 * conflicts between user vertices and item vertices
 * 
 * @author Han JU
 *
 */
public class TripleEdgeInputFormat
extends TextEdgeInputFormat<VLongWritable, VLongWritable> {
	
	private static final Pattern SEP = Pattern.compile("\t");
	
	@Override
	public EdgeReader<VLongWritable, VLongWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new TripleEdgeReader();
	}
	
	class TripleEdgeReader
	extends TextEdgeReaderFromEachLineProcessed<Long[]> {

		@Override
		protected Long[] preprocessLine(Text line) throws IOException {
			Long[] pair = new Long[2];
			String[] l = SEP.split(line.toString());
			pair[0] = Long.parseLong(l[0]);		// user
			pair[1] = Long.parseLong(l[1]);		// product
			return pair;
		}

		@Override
		protected VLongWritable getTargetVertexId(Long[] pair)
				throws IOException {
			return new VLongWritable(pair[1]);
		}

		@Override
		protected VLongWritable getSourceVertexId(Long[] pair)
				throws IOException {
			// negative vertex id for user node
			return new VLongWritable(-pair[0]);
		}

		@Override
		protected VLongWritable getValue(Long[] line) throws IOException {
			return new VLongWritable(-1);		// nothing for the link
		}
	}
}
