package com.kxen.han.projection.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

/**
 * Serializable class for a transaction list of items
 * Use VLongWritable, reduce serialized file size
 * 
 * @author Han JU
 *
 */
public class TransactionWritable implements Writable, Iterable<Long> {
	
	private static final Joiner joiner = Joiner.on(" ").skipNulls();
	
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
	
	public int size() {
		return size;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		transacList = new long[size];
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.readFields(in);
			transacList[i] = vLongWriter.get();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.set(transacList[i]);
			vLongWriter.write(out);
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
		String[] res = new String[size];
		for (int i = 0; i < size; i++) {
			res[i] = Long.toString(transacList[i]);
		}
		return joiner.join(res);
	}
}