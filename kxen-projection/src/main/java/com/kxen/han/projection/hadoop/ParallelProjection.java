package com.kxen.han.projection.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;


/**
 * Parallel bipartite graph projection on Hadoop
 * Based on paper: http://infolab.stanford.edu/~echang/recsys08-69.pdf
 * 
 * @author Han JU
 *
 */
public class ParallelProjection {
	
	private static final Logger log = LoggerFactory.getLogger(ParallelProjection.class);
	
	public static final String F_LIST = "f-list";
	public static final String G_LIST = "g-list";
	public static final String MIN_SUPPORT = "minSupport";
	public static final String NUM_GROUP = "numGroup";
	public static final String PARALLEL_COUNTING = "parallel-counting";
	public static final String PRE_PROCESSING = "pre-processing";
	public static final String PARALLEL_PROJECTION = "parallel-projection";
	public static final String FILE_PATTERN = "part-*";
	
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
			int numGroup) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
		conf.set(MIN_SUPPORT, Integer.toString(minSupport)); 
		conf.set(NUM_GROUP, Integer.toString(numGroup));
		Stopwatch sw = new Stopwatch(); 

		// step 0, pre-processing
		// change pair representation to transaction
		sw.start();
		startPreProcessing(input, tmp, conf); 
		sw.stop();
		log.info("Pre-processing finished, took {} ms ...", sw.elapsed(TimeUnit.MILLISECONDS)); 

		// step 1, parallel counting
		// output counts (singleton support) at {tmp}/{PRE_PROCESSING}
		sw.reset().start();
		String transacInput = tmp+"/"+PRE_PROCESSING;
		startParallelCounting(transacInput, tmp, conf); 
		sw.stop();
		log.info("Parallel counting finished, took {} ms ...", sw.elapsed(TimeUnit.MILLISECONDS));
		
		// TODO step 2, grouping
		// read counting results, generate F-list, group it into G-list
		// single machine
		
		sw.reset().start();
		// read
		FileSystem fs = FileSystem.get(conf);
		// TODO how to handle multiple files !!!
		BufferedReader br = new BufferedReader(
				new InputStreamReader(fs.open(new Path(tmp+"/"+PARALLEL_COUNTING+"/"+"part-r-00000"))));
		String line;
		Map<Long, Long> freq = Maps.newHashMap();
		while ((line = br.readLine()) != null) {
			String[] pair = line.split("\t");
			long count = Long.parseLong(pair[1]);
			long item = Long.parseLong(pair[0]);
			
			if (count >= minSupport) {
				freq.put(item, count);
			}
		}
		br.close();
		
		// save to HDFS
		Path fListPath = new Path(tmp, F_LIST); 
		OutputStream out = fs.create(fListPath);
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(freq);
		oos.close();
		DistributedCache.addCacheFile(fListPath.toUri(), conf);		// add to dCache
		sw.stop();
		log.info("took {} ms ...", sw.elapsed(TimeUnit.MILLISECONDS));
	
		// TODO step 3, parallel FP-Tree
		// build local FP-Tree and do partial projection
		sw.reset().start();
		startParallelProjection(transacInput, tmp, conf);
		sw.stop();
		log.info("Parallel projection finished, took {} ms ...", sw.elapsed(TimeUnit.MILLISECONDS));
		
		// TODO step4, aggregation
		// aggregate partial projection results
	}
	
	/**
	 * Delete a folder with its containing files on HDFS
	 * @param toDelete
	 */
	public static void delete(Path toDelete, Configuration conf) throws IOException {
		FileSystem.get(conf).delete(toDelete, true);
	}
	
	/**
	 * Start step 0, pre-processing 
	 */
	public static void startPreProcessing(
			String input,
			String tmp,
			Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Pre processing with input:" + input);
		job.setJarByClass(ParallelProjection.class);

	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(input));
	    Path output = new Path(tmp, PRE_PROCESSING);
	    
	    // delete existing tmp folder
	    delete(output, conf);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(PreProcessingMapper.class);
	    job.setReducerClass(PreProcessingReducer.class);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
	}
	
	/**
	 * Start step 1, parallel counting of items
	 */
	public static void startParallelCounting(
			String input,
			String tmp,
			Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
	    
	    Job job = new Job(conf, "Parallel counting with input: " + input);
	    job.setJarByClass(ParallelProjection.class); 	// what use ???
	    
	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(input));
	    Path output = new Path(tmp, PARALLEL_COUNTING);
	    
	    // delete existing tmp folder
	    delete(output, conf);
	    
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
	
	/**
	 * Start step 3, parallel projection 
	 */
	public static void startParallelProjection(
			String input,
			String tmp,
			Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "Parallel projection with input: " + input);
		job.setJarByClass(ParallelProjection.class);
		
	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(input));
	    Path output = new Path(tmp, PARALLEL_PROJECTION);
	    
	    // delete existing tmp folder
	    delete(output, conf);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(TransactionWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(ParallelProjectionMapper.class);
	    job.setReducerClass(ParallelProjectionReducer.class);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
	}
}