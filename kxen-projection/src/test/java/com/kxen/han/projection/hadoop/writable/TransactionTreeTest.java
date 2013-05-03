package com.kxen.han.projection.hadoop.writable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransactionTreeTest {
	@Test
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
	public void testTransactionTreeIteratorOneElemCase() {
		TransactionTree tt = new TransactionTree();
		tt.insertTransac(Arrays.asList(new Long[]{5l}), 1);
		
		Iterator<Pair<List<Long>, Long>> iter = 
				new TransactionTreeIterator(tt);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
}