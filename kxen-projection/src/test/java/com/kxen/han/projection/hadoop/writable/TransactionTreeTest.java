package com.kxen.han.projection.hadoop.writable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.kxen.han.projection.fpg2.Projection;

@RunWith(JUnit4.class)
public class TransactionTreeTest {
	//@Test
	public void testTransactionTreeIteratorNormalCase() {
		TransactionTree tt = new TransactionTree();
		tt.insertTransac(Arrays.asList(new Long[]{1l,2l,3l,4l,5l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{1l,2l,3l,4l,5l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{1l,2l,3l,4l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{1l,2l,3l,6l,7l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{3l,4l,5l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{5l}), 1);
		
		Iterator<Pair<List<Long>, Long>> iter = 
				new TransactionTreeIterator(tt);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	@Test
	public void testTransactionTreeIteratorSecondCase() {
		TransactionTree tt = new TransactionTree();
		tt.insertTransac(Arrays.asList(new Long[]{6l,3l,1l,13l,16l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{6l,3l,1l,2l,13l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{6l,2l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{3l,2l,16l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{6l,3l,1l,13l,16l}), 1);
		
		Iterator<Pair<List<Long>, Long>> iter = 
				new TransactionTreeIterator(tt);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	//@Test
	public void testTransactionTreeIteratorOneElemCase() {
		TransactionTree tt = new TransactionTree();
		tt.insertTransac(Arrays.asList(new Long[]{5l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{6l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{7l}), 1);
		tt.insertTransac(Arrays.asList(new Long[]{8l}), 1);
		

		Iterator<Pair<List<Long>, Long>> iter = 
				new TransactionTreeIterator(tt);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	//@Test
	public void testWithMSD() throws Exception {
		DataModel dataModel = new GenericBooleanPrefDataModel(
				GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
						new File("src/main/resources/test_triples"))));
		
		Projection proj = new Projection(dataModel, 2);
		proj.project("/home/port/outputs/project_test");
	}
}