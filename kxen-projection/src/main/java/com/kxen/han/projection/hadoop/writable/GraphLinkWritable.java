package com.kxen.han.projection.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class GraphLinkWritable implements Writable {
	
	private static final String SEP = "\t";
	private static VIntWritable viw = new VIntWritable();
	private static VLongWritable vlw = new VLongWritable();

	private int item1;
	private int item2;
	private long support;
		
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
		viw.readFields(in); item1 = viw.get();
		viw.readFields(in); item2 = viw.get();
		vlw.readFields(in); support = vlw.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		viw.set(item1); viw.write(out);
		viw.set(item2); viw.write(out);
		vlw.set(support); vlw.write(out);
	}
	
	@Override
	public String toString() {
		return Integer.toString(item1) + SEP
				+ Integer.toString(item2) + SEP
				+ Long.toString(support);
	}
}