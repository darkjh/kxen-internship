package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TransactionVertexInputFormat
extends TextVertexInputFormat<LongWritable, BooleanWritable, NullWritable> {

	private static final Pattern USER_SEP = Pattern.compile("\t");
	private static final Pattern PROD_SEP = Pattern.compile(" ");

	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) {
		return new TransactionVertexReader();
	}
	
	/**
	 * VertexReader that features <code>double</code> vertex values and
	 * <code>float</code> out-edge weights. The files should be in the
	 * following JSON format: JSONArray(<vertex id>, <vertex value>,
	 * JSONArray(JSONArray(<dest vertex id>, <edge value>), ...)) Here is an
	 * example with vertex id 1, vertex value 4.3, and two edges. First edge
	 * has a destination vertex 2, edge value 2.1. Second edge has a
	 * destination vertex 3, edge value 0.7. [1,4.3,[[2,2.1],[3,0.7]]]
	 */
	class TransactionVertexReader
	extends TextVertexReaderFromEachLineProcessed<Pair<Long,String[]>> {
		
		@Override
		protected Pair<Long, String[]> preprocessLine(Text line)
				throws IOException {
			String[] tmp = USER_SEP.split(line.toString());
			long user = Long.parseLong(tmp[0]);
			String[] prods = PROD_SEP.split(tmp[1]);
			return Pair.of(user, prods);
		}

		@Override
		protected LongWritable getId(Pair<Long, String[]> line)
				throws IOException {
			
			return null;
		}

		@Override
		protected BooleanWritable getValue(Pair<Long, String[]> line)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
				Pair<Long, String[]> line) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
