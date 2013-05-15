package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TripleEdgeInputFormat
extends TextEdgeInputFormat<LongWritable, LongWritable> {
	
	private static final Pattern SEP = Pattern.compile("\t");
	
	@Override
	public EdgeReader<LongWritable, LongWritable> createEdgeReader(
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
		protected LongWritable getTargetVertexId(Long[] pair)
				throws IOException {
			return new LongWritable(pair[1]);
		}

		@Override
		protected LongWritable getSourceVertexId(Long[] pair)
				throws IOException {
			return new LongWritable(pair[0]);
		}

		@Override
		protected LongWritable getValue(Long[] line) throws IOException {
			return new LongWritable(0);		// nothing for the link
		}	
	}
}
