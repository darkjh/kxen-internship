package com.kxen.han.projection.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Parallel bipartite graph projection on Hadoop
 * Based on paper: http://infolab.stanford.edu/~echang/recsys08-69.pdf
 * 
 * @author Han JU
 *
 */
public class ParallelProjection {
	
	private static final String F_LIST = "f-list";
	private static final String PARALLEL_COUNTING = "parallel-counting";
	
	private ParallelProjection() {}
	
	/**
	 * Run every step of parallel bipartite graph projection
	 * 	- parallel counting
	 * 	- single machine grouping
	 * 	- parallel FP-Tree projection
	 * 	- aggregating partial results 
	 * 
	 * @param input			input path
	 * @param output		output path
	 * @param tmp			temp directory path
	 * @param minSupport	min. support threshold
	 * @param groupNum		#group that frequent items will be grouped
	 */
	public static void runProjection(
			String input, 
			String output,
			String tmp,
			int minSupport,
			int groupNum) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		// TODO pre-processing, change pair representation to transaction
		// Pig ???
		
		// step 1
		startParallelCounting(input, tmp, conf);
		
		// TODO step 2
		// reading F-list, grouping into G-list
		// sinlge machine
		
		// TODO step 3
		// reading F-list, G-list, build local FP-Tree and do partial projection
		
		// TODO step4
		// aggregating partial projection results
	}
	
	/**
	 * Start step 1, parallel counting of items
	 */
	public static void startParallelCounting(
			String input,
			String tmp,
			Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
	    
		conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    
	    Job job = new Job(conf, "Parallel counting with input: " + input);
	    job.setJarByClass(ParallelProjection.class); 	// what use ???
	    
	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(input));
	    Path output = new Path(tmp, PARALLEL_COUNTING);
	    // TODO delete existing tmp folder
	    FileOutputFormat.setOutputPath(job, output);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(ParallelCountingMapper.class);
	    job.setCombinerClass(ParallelCountingReducer.class);
	    job.setReducerClass(ParallelCountingReducer.class);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
	}
}