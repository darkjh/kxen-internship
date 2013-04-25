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
	
	public TransactionWritable(Iterable<Long> transac, int size) {
		this.size = size;
		transacList = new long[size];
		int i = 0;
		for (Long l : transac) {
			transacList[i++] = l;
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		transacList = new long[size];
		
		for (int i = 0; i < size; i++) {
			transacList[i] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			out.writeLong(transacList[i]);
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
