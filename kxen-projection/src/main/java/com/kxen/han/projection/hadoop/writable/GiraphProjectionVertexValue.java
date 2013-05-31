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
	public long[] neighbors;
	
	static public GiraphProjectionVertexValue newInstance() {
		return new GiraphProjectionVertexValue();
	}
	
	public GiraphProjectionVertexValue() {
		size = 0;
		round = 0;
	}
	
	public GiraphProjectionVertexValue(int round, long[] neighbors) {
		this.round = round;
		this.size = neighbors.length;
		this.neighbors = new long[this.size];
		System.arraycopy(neighbors, 0, this.neighbors, 0, this.size);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		round = in.readInt();
		size = in.readInt();
		neighbors = new long[size];
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.readFields(in);
			neighbors[i] = vLongWriter.get();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(round);
		out.writeInt(size);
		VLongWritable vLongWriter = new VLongWritable();
		for (int i = 0; i < size; i++) {
			vLongWriter.set(neighbors[i]);
			vLongWriter.write(out);
		}
	}
}
