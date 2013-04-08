package com.kxen.han.projection.fpg;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OutputLayerTest {
	private String fileout;
	private OutputLayer olf;
	private OutputLayer olc;
	private String msg = "simple message";
	
	@Before
	public void setUp() throws Exception {
		fileout = "out";
		olf = new OutputLayer(fileout);
		olc = new OutputLayer();
	}
	
	@After
	public void tearDown() {
		File out = new File(fileout);
		if (out.exists())
			out.delete();
	}
	
	@Test
	public void testFileOutput() throws Exception {
		olf.writeLine(msg);
		olf.close();
		BufferedReader br = new BufferedReader(new FileReader(new File(fileout)));
		assertEquals(br.readLine(), msg);
	}
}
