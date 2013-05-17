package com.kxen.han.projection.giraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import com.google.common.collect.Lists;

public class GiraphProjectionTest extends BspCase {

	public GiraphProjectionTest() {
		super(GiraphProjectionTest.class.toString());
	}
	
	public String[] parseInput(File input)
			throws FileNotFoundException {
		List<String> triples = Lists.newArrayList();
		Scanner scanner = new Scanner(input);
		while (scanner.hasNextLine()) {
			triples.add(scanner.nextLine());
		}
		String[] res = new String[triples.size()];
		for (int i = 0; i < triples.size(); i++) {
			res[i] = triples.get(i);
		}
		return res;
	}

	@Test
	public void testNormalCase() throws Exception {
		String[] edges = parseInput(new File("src/test/resources/TestExampleAutoGen"));

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setInt(ProjectionVertex.MIN_SUPPORT, 3);
		conf.setVertexClass(ProjectionVertex.class);
		conf.setOutEdgesClass(ByteArrayEdges.class);
		conf.setEdgeInputFormatClass(TripleEdgeInputFormat.class);
		conf.setVertexOutputFormatClass(ProjectedGraphOutputFormat.class);
		Iterable<String> results = InternalVertexRunner.run(conf, null, edges);
		
		for (String s : results) {
			if (!s.isEmpty())
				System.out.println(s);
		}
	}
}
