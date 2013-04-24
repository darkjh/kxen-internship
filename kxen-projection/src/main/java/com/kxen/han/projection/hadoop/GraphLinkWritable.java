package com.kxen.han.projection.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class GraphLinkWritable implements Writable {

	int item1;
	int item2;
	long support;
	
	public GraphLinkWritable() {
		item1 = -1;
		item2 = -1;
		support = -1;
	}
	
	public GraphLinkWritable(int item1, int item2, long supp) {
		this.item1 = item1;
		this.item2 = item2;
		this.support = supp;
	}
	
	public int getItem2() {
		return item2;
	}
	
	public long getSupport() {
		return support;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		IntWritable iw = new IntWritable();
		LongWritable lw = new LongWritable();
		iw.readFields(in);
		item1 = iw.get();
		iw.readFields(in);
		item2 = iw.get();
		lw.readFields(in);
		support = lw.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		IntWritable iw = new IntWritable();
		LongWritable lw = new LongWritable();
		iw.set(item1);
		iw.write(out);
		iw.set(item2);
		iw.write(out);
		lw.set(support);
		lw.write(out);
	}
	
	@Override
	public String toString() {
		return Integer.toString(item1) + " : "
				+ Integer.toString(item2) + " : "
				+ Long.toString(support);
	}
}
