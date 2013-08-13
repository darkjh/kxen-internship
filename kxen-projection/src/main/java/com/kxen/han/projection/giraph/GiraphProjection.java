package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.kxen.han.projection.pfpg.ParallelProjection;

/**
 * A driver class to run the Giraph based projection
 * Handles:
 *   - start stage (1, 2)
 *   - projection side (user, product)
 *   - mega-hub detection
 * 
 * @author Han JU
 *
 */
public class GiraphProjection 
extends Configured implements Tool {
	
	private static final Logger log = LoggerFactory.getLogger(GiraphProjection.class);
	
	/** CLI interface set-up */
	static public final String IN = "i";
	static public final String OUT = "o";
	static public final String SUPP = "s";
	static public final String TMP = "tmp";
	static public final String WORKER = "w";
	static public final String GROUP = "g";
	static public final String START = "startFrom";
	static public final String USER_SPACE = "userSpace";
	static public final String MEGA_HUB = "megaHub";
	
	public static final String TRIPLE_COUNTING = "triple-counting";
	public static final String F_LIST = "giraph-f-list"; 
	public static final String FILE_PATTERN = "part-r-*";
	
	public static final String PROJ_SIDE = "-proj";
	public static final String EMIT_SIDE = "-emit";
	
	private static Options OPTIONS;
	static {
		OPTIONS = new Options();
		OPTIONS.addOption(IN, "input", true, "Input Path");
		OPTIONS.addOption(OUT, "output", true, "Output Path");
		OPTIONS.addOption(TMP, "tempDir", true, "Temp Directory");
		OPTIONS.addOption(SUPP, "minSupport", true, "Minimum Support Threshold");
		OPTIONS.addOption(WORKER, "worker", true, "Number of Wokers To Use");
		OPTIONS.addOption(GROUP, "numGroup", true, "Number of groups");
		OPTIONS.addOption(START, true, "Start Directly From a Step");
		OPTIONS.addOption(USER_SPACE, false, "Project to user space");
		OPTIONS.addOption(MEGA_HUB, false, "Mega-hub detection");
    }
	
	/** In terms of message-passing, there's emit-side and project-side nodes*/
	public enum Side {
		PROJ, EMIT
	}
	
	/**
	 * Counting of individual support (frequency) 
	 * 
	 * @param side tells the program which side it is counting for, 
	 *             controls the output name
	 * @param conf config object of the job, contains -userSpace user param
	 *             decides which column of the edge file will be counted
	 */
	public static void startParallelCounting(
			Side side,
			GiraphConfiguration conf) 
			throws IOException, InterruptedException, ClassNotFoundException {
	    conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    	    
	    Job job = new Job(conf, "GiraphProjection: Counting");
	    job.setJarByClass(GiraphProjection.class); 

	    FileInputFormat.addInputPath(job, new Path(conf.get(IN)));

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(TripleCounting.TripleCountingMapper.class);
	    job.setCombinerClass(TripleCounting.TripleCountingReducer.class);
	    job.setReducerClass(TripleCounting.TripleCountingReducer.class);
	    job.setNumReduceTasks(4);
	    
	    String path = TRIPLE_COUNTING + (side == Side.PROJ ? PROJ_SIDE : EMIT_SIDE);
	    Path output = new Path(conf.get(TMP)+"/"+path);
	    ParallelProjection.delete(output, conf);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Triple counting failed ...");
	    }
	}
	
	/**
	 * Reading the result of parallel counting, transform it into bitmap and 
	 * dispatch it to DCache
	 * 
	 * @param side controls the behavior of this method:
	 *               - PROJ: means projection side, serialize then dispatch
	 *               - EMIT: means emit side, serialize then dispatch
	 */
	public static void generateFList(
			Side side,
			int minSupport,
			Configuration conf) throws IOException {
		String tmp = conf.get(TMP);
		String path = TRIPLE_COUNTING + 
				(side == Side.PROJ ? PROJ_SIDE : EMIT_SIDE);
		boolean megaHub = isMegaHub(conf);
		Map<Long,Long> freq = 
				readCountingResults(tmp+"/"+path, minSupport, conf);
		BitSet bs = new BitSet();
		
		if (side == Side.PROJ) {
			System.out.println("\nFor projection side nodes: ");
			long threshold = (long)graphStats(freq);
			if (megaHub) {
				for (Entry<Long,Long> entry : freq.entrySet()) {
					if (entry.getValue() <= threshold) {
						bs.set(entry.getKey().intValue());
					}
				}
			} else {
				for (Long l : freq.keySet()) {
					bs.set(l.intValue());
				}
			}
			dispatchFList(tmp+"/"+F_LIST+PROJ_SIDE, bs, conf);
		} else {	// emit side
			// eventually we may have different process for this part
			System.out.println("\nFor emit side nodes: ");
			long threshold = (long)graphStats(freq);
			if (megaHub) {
				for (Entry<Long,Long> entry : freq.entrySet()) {
					if (entry.getValue() <= threshold) {
						bs.set(entry.getKey().intValue());
					}
				}
			} else {
				for (Long l : freq.keySet()) {
					bs.set(l.intValue());
				}
			}
			dispatchFList(tmp+"/"+F_LIST+EMIT_SIDE, bs, conf);
		}
		System.out.println("Succesfully dispatched file to DCache ...");
		System.out.println("Remaining users/prods: " + bs.cardinality());
		System.out.println("Filtered mega-hubs : " + (freq.size()-bs.cardinality()));
	}
	
	/** Reads counting results and keep only frequent items in a Map */
	public static Map<Long,Long> readCountingResults(
			String path, 
			int minSupport,
			Configuration conf) throws IOException { 
		FileSystem fs = FileSystem.get(conf);
		LongWritable key = new LongWritable(); 
		LongWritable value = new LongWritable(); 
		Map<Long, Long> freq = Maps.newHashMap();
		Path countResult = new Path(path, FILE_PATTERN);
		FileStatus[] status = fs.globStatus(countResult);
		for (FileStatus s : status) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, s.getPath(), conf);
			while (reader.next(key, value)) {
				if (value.get() >= minSupport) {
					freq.put(key.get(), value.get());
				}
			}
			reader.close();
		}
		return freq;
	}
	
	/** Dispatch the f-list using DCache */
	public static void dispatchFList(
			String path, 
			BitSet bs,
			Configuration conf) throws IOException {
		// save to HDFS
		FileSystem fs = FileSystem.get(conf);
		Path fListPath = new Path(path); 
		OutputStream out = fs.create(fListPath);
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(bs);
		oos.close();
		// add to dCache, available locally for all compute nodes
		DistributedCache.addCacheFile(fListPath.toUri(), conf);	
	}
	
	/** Statistics for the graph, generated from the counting results */
	public static double graphStats(Map<Long, Long> freq) {
		Variance var = new Variance();
		Mean mean = new Mean();
		for (Long l : freq.values()) {
			double dv = l.doubleValue();
			var.increment(dv);
			mean.increment(dv);
		}
		
		if (log.isInfoEnabled()) { 
			System.out.println("Frequent Items: " + freq.size());
			System.out.println("Average Neighbors: " + mean.getResult());
			System.out.println("Variance: " + var.getResult());
		}
		
		return megaHubThreshold(mean.getResult(), var.getResult());
	}
	
	/** formula for mega-hub definition */
	public static double megaHubThreshold(double mean, double var) {
		return (mean + 5*Math.sqrt(var));
	}
	
	public static boolean isUserSpace(Configuration conf) {
		return conf.getBoolean(USER_SPACE, false);
	}
	
	public static boolean isMegaHub(Configuration conf) {
		return conf.getBoolean(MEGA_HUB, false);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(OPTIONS, args);
		
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		
		giraphConf.set(IN, cmd.getOptionValue(IN));
		giraphConf.set(OUT, cmd.getOptionValue(OUT));
		giraphConf.set(TMP, cmd.getOptionValue(TMP));
		giraphConf.set(SUPP, cmd.getOptionValue(SUPP));
		giraphConf.set(GROUP, cmd.getOptionValue(GROUP));
		
		giraphConf.setBoolean(MEGA_HUB, cmd.hasOption(MEGA_HUB));
		giraphConf.setBoolean(USER_SPACE, cmd.hasOption(USER_SPACE));
		giraphConf.set(SUPP, cmd.getOptionValue(SUPP));
		
		int start = Integer.parseInt(cmd.getOptionValue(START, "1"));
		
		// first step, parallel counting
		if (start <= 1) {
			// normally, only count the projection side
			// which is decided by -userSpace param
			startParallelCounting(Side.PROJ, giraphConf);
			// if mega-hub detection, also need to count the other side
			if (isMegaHub(giraphConf)) {
				// flip the -userSpace param to count the other side
				GiraphConfiguration filppedConf = new 
						GiraphConfiguration(giraphConf);
				filppedConf.setBoolean(USER_SPACE, 
						!filppedConf.getBoolean(USER_SPACE, false));
				startParallelCounting(Side.EMIT, filppedConf);
			}
		}
		
		// second step, generate f-list and graph stats for filtering
		if (start <= 2) {
			generateFList(Side.PROJ, giraphConf.getInt(SUPP, 2), giraphConf);
			if (isMegaHub(giraphConf)) {
				// for the emit-side, no filtering on support, just mega-hub
				generateFList(Side.EMIT, 0, giraphConf);
			}
		}
		
		// giraph related settings
		int worker = Integer.parseInt(cmd.getOptionValue(WORKER));
		giraphConf.setComputationClass(ProjectionComputation.class);
		giraphConf.setEdgeInputFormatClass(TripleEdgeInputFormat.class);
		giraphConf.setVertexOutputFormatClass(ProjectedGraphVertexOutputFormat.class);
		giraphConf.setWorkerConfiguration(worker, worker, 100.0f);
		giraphConf.setDoOutputDuringComputation(true);
		giraphConf.setEdgeInputFilterClass(EdgeBySupportFilter.class);
		giraphConf.setMasterComputeClass(ProjectionMasterCompute.class);

		GiraphJob job = new GiraphJob(giraphConf, "GiraphProjection: Projection "
                +"with "+cmd.getOptionValue(GROUP)+" groups");

		GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(cmd.getOptionValue(IN)));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue(OUT)));

		// launch the job on hadoop
		return job.run(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GiraphProjection(), args);
	}
}