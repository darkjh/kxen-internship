package com.kxen.han.projection.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

/**
 * A compact representation of frequent item pairs
 * Used by parallel projection step to output results for aggregation step
 * 
 * @author Han JU
 *
 */
public class GraphLinksWritable implements Writable {
	
	private static VLongWritable longWriter = new VLongWritable();
	private static VIntWritable intWriter = new VIntWritable();
	
	private int item;
	private int size;
	private int[] others;
	private long[] supports;
	
	public GraphLinksWritable() {
		item = -1;
		size = 0;
	}
	
	public GraphLinksWritable(int item, List<Pair<Integer, Long>> others) {
		this.item = item;
		this.size = others.size();
		this.others = new int[size];
		this.supports = new long[size];
		int i = 0;
		for (Pair<Integer, Long> other : others) {
			this.others[i] = other.getKey();
			this.supports[i] = other.getValue();
			i++;
		}
	}
	
	public int getItem() {
		return item;
	}
	
	public int getSize() {
		return size;
	}

	public int getOther(int index) {
		return others[index];
	}

	public long getSupport(int index) {
		return supports[index];
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		intWriter.readFields(in); item = intWriter.get();
		intWriter.readFields(in); size = intWriter.get();
		others = new int[size];
		supports = new long[size];
		for (int i = 0; i < size; i++) {
			intWriter.readFields(in); others[i] = intWriter.get();
		}
		for (int i = 0; i < size; i++) {
			longWriter.readFields(in); supports[i] = longWriter.get();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		intWriter.set(item); intWriter.write(out);
		intWriter.set(size); intWriter.write(out);
		for (int i : others) {
			intWriter.set(i); intWriter.write(out);
		}
		for (long l : supports) {
			longWriter.set(l); longWriter.write(out);
		}
	}
	
	
}