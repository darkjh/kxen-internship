package com.kxen.han.projection.giraph;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Maps;

@RunWith(JUnit4.class)
public class GraphStatTest {
	
	Map<Long, Long> freq;
	
	public void populateFreqList(long[] input) {
		freq = Maps.newHashMap();
		long k = 0;
		for (long l : input) {
			freq.put(k++, l);
		}
	}
	
	public void testStats(long[] input, double mean, double var) {
		populateFreqList(input);

		assertEquals(
				GiraphProjection.graphStats(freq),
				GiraphProjection.megaHubThreshold(mean, var), 
				0d);
	}
	
	@Test
	public void testProjectionSideStats1() {
		long[] input = new long[]{1l,2l,3l,4l,5l};
		testStats(input, 3d, 2.5d);
	}
	
	@Test
	public void testProjectionSideStats2() {
		long[] input = new long[]{1l,1l,1l,1l,1l};
		testStats(input, 1d, 0d);
	}
}
