package com.kxen.han.projection.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class GiraphProjectionVertexValue 
implements Writable {
	
	public int round;
	public int size;
	public boolean containsValue;
	public long[] neighbors;
	public long[] values;
	
	static public GiraphProjectionVertexValue newInstance() {
		return new GiraphProjectionVertexValue();
	}
	
	public GiraphProjectionVertexValue() {
		size = 0;
		round = 0;
	}
	
	/** used by user vertices */
	public GiraphProjectionVertexValue(int round, long[] neighbors) {
		this.round = round;
		this.size = neighbors.length;
		this.containsValue = false;
		this.neighbors = new long[this.size];
		System.arraycopy(neighbors, 0, this.neighbors, 0, this.size);
	}
	
	/** used by product vertices */
	public GiraphProjectionVertexValue(long[] neighbors, long[] values, int size) {
		this.round = -1;
		this.size = size;
		this.containsValue = true;
		this.neighbors = new long[this.size];
		this.values = new long[this.size];
		System.arraycopy(neighbors, 0, this.neighbors, 0, this.size);
		System.arraycopy(values, 0, this.values, 0, this.size);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		round = in.readInt();
		size = in.readInt();
		containsValue = in.readBoolean();
		neighbors = new long[size];
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.readFields(in);
			neighbors[i] = vLongWriter.get();
		}
		if (containsValue) {
			values = new long[size];
			for (int i = 0; i < size; i++) {
				vLongWriter.readFields(in);
				values[i] = vLongWriter.get();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(round);
		out.writeInt(size);
		out.writeBoolean(containsValue);
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.set(neighbors[i]);
			vLongWriter.write(out);
		}
		if (containsValue) {
			for (int i = 0; i < size; i++) {
				vLongWriter.set(values[i]);
				vLongWriter.write(out);
			}
		}
	}
}