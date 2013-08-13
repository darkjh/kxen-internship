package com.kxen.han.projection.giraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
	
	public void projectionTest(
			String[] edges, 
			String outputFileName,
			int support) throws Exception {
		String outDir = "/home/port/output/giraph-unit-test/";
		File dir = new File(outDir);
		dir.mkdirs();
		File output = new File(dir, outputFileName);
		BufferedWriter bw = new BufferedWriter(
				new FileWriter(output));

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setInt(ProjectionComputation.MIN_SUPPORT, support);
		conf.setBoolean("giraph.doOutputDuringComputation", true);
		
		conf.setComputationClass(ProjectionComputation.class);
		conf.setMasterComputeClass(ProjectionMasterCompute.class);
		conf.setOutEdgesClass(ByteArrayEdges.class);
		conf.setEdgeInputFormatClass(TripleEdgeInputFormat.class);
		conf.setVertexOutputFormatClass(ProjectedGraphVertexOutputFormat.class);
		conf.setBoolean(GiraphProjection.USER_SPACE, false);

        conf.setWorkerConfiguration(2, 2, 1.0f);
        conf.set("giraph.useOutOfCoreGraph", "true");
        conf.set("giraph.maxPartitionsInMemory", "1");

		Iterable<String> results = InternalVertexRunner.run(conf, null, edges);
		
		for (String s : results) {
			if (!s.isEmpty()) {
//				System.out.println(s);
				bw.write(s+"\n");
			}
		}
		bw.close();
		
	}

	//@Test
	public void testPaperCase() throws Exception {
		String[] edges = parseInput(new File("src/test/resources/TestExampleAutoGen"));
		projectionTest(edges, "paper-case-result", 3);
	}
	
	@Test
	public void testMSDTestCase() throws Exception {
		String[] edges = parseInput(new File("src/main/resources/test_triples"));
		projectionTest(edges, "msd-test-result", 2);
	}
}
