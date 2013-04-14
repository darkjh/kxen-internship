package com.kxen.han.projection.fpg2;

import java.io.File;

import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.kxen.han.projection.fpg.OutputLayer;

@RunWith(JUnit4.class)
public class Projection2Test {

	@Test
	public void testProject() throws Exception {
		DataModel dataModel = new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
						new File("src/test/resources/TestExampleAutoGen"))));
		Projection proj = new Projection(dataModel, 3);
		// TODO fix test
		// proj.project();
	}
}
