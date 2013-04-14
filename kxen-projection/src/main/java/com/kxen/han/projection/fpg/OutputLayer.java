package com.kxen.han.projection.fpg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

public class OutputLayer {
	private PrintWriter pw;
	
	/* static factory */
	public static OutputLayer newInstance() {
		return new OutputLayer();
	}
	
	/* static factory */
	public static OutputLayer newInstance(String filepath) 
			throws Exception {
		return new OutputLayer(filepath); 
	}
	
	/** ctor for console output */
	private OutputLayer() {
		pw = new PrintWriter(System.out);
	}
	
	/** ctor for file output */
	private OutputLayer(String filepath) throws Exception {
		this(new File(filepath));
	}
	
	/** ctor for file output */
	private OutputLayer(File file) throws Exception {
		pw = new PrintWriter(new BufferedWriter(new FileWriter(file)));
	}
	
	public void writeLine(String s) {
		pw.println(s);
	}
	
	public void close() {
		pw.close();
	}
}
