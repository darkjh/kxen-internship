package com.kxen.han.projection.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
		item1 = in.readInt();
		item2 = in.readInt();
		support = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(item1);
		out.writeInt(item2);
		out.writeLong(support);
	}
	
	@Override
	public String toString() {
		return Integer.toString(item1) + " : "
				+ Integer.toString(item2) + " : "
				+ Long.toString(support);
	}
}
