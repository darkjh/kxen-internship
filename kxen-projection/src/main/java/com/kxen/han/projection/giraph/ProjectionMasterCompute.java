package com.kxen.han.projection.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;

/**
 * Master computation class is used to:
 *   - stops the computation when all groups have processed
 *   - log the total msg length in each super-step
 * 
 * @author Han JU
 *
 */
public class ProjectionMasterCompute
extends MasterCompute {
	
	public static final String MSG_LEN_COUNT = "msgLenCount";
	public static final String NEIGHBOR_COUNT = "neighborCount";
	public static String ROUND = GiraphProjection.GROUP;
	
	private static int TOTAL_ROUND;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		throw new IOException();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new IOException();
	}

	@Override
	public void compute() {
		// print total msg length for each super-step
		long sum = ((LongWritable) getAggregatedValue(MSG_LEN_COUNT)).get();
		System.out.println(sum);
		
		// coordination of computation
		long superstep = getSuperstep();
		if (TOTAL_ROUND + 1 == superstep) {
			// halts computation if all groups have processed
			// + 1 for the first mega hub computation
			haltComputation();
		}
	}

	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		TOTAL_ROUND = getConf().getInt(ROUND, 20);
		// total msg length aggregator, one for each super-step
		registerAggregator(MSG_LEN_COUNT, LongSumAggregator.class);
		registerAggregator(NEIGHBOR_COUNT, IntSumAggregator.class);
	}
}