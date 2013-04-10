package com.kxen.han.projection.fpg;

import java.io.File;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.kxen.han.projection.TestExampleGen;


@RunWith(JUnit4.class)
public class ProjectionTest {

	@Test
	public void testProject() throws Exception {
		TestExampleGen.generateFromTransacFile(new File("src/test/resources/nodeTestTransac"));
		OutputLayer ol = new OutputLayer();
		Projection.project("src/test/resources/TestExampleAutoGen", 
				ol, 3);
	}
}
