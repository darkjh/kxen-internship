package com.kxen.han.projection.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Arrays;

public class TransactionWritable implements Writable, Iterable<Long> {
	
	int size;
	long[] transacList;
	
	public TransactionWritable() {
		size = 0;
	}
	
	public TransactionWritable(List<Long> transac) {
		size = transac.size();
		transacList = new long[size];
		for (int i = 0; i < size; i++) {
			transacList[i] = transac.get(i);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		LongWritable lw = new LongWritable();
		IntWritable iw = new IntWritable();
		
		iw.readFields(in);
		size = iw.get();
		transacList = new long[size];
		
		for (int i = 0; i < size; i++) {
			lw.readFields(in);
			transacList[i] = lw.get();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		LongWritable lw = new LongWritable();
		new IntWritable(size).write(out);
		
		for (int i = 0; i < size; i++) {
			lw.set(transacList[i]);
			lw.write(out);
		}
	}

	@Override
	public Iterator<Long> iterator() {
		return new Iterator<Long>() {
			int curr = 0;
			
			@Override
			public boolean hasNext() {
				return curr < size;
			}

			@Override
			public Long next() {
				return transacList[curr++];
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	@Override
	public String toString() {
		return Arrays.toString(transacList);
	}
}
