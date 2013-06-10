package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
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

import com.google.common.collect.Maps;
import com.kxen.han.projection.pfpg.ParallelProjection;

/**
 * A driver class to run the Giraph based projection
 * 
 * @author Han JU
 *
 */
public class GiraphProjection 
extends Configured implements Tool {
	
	/** cli interface set-up */
	static public final String IN = "i";
	static public final String OUT = "o";
	static public final String SUPP = "s";
	static public final String TMP = "tmp";
	static public final String WORKER = "w";
	static public final String GROUP = "g";
	static public final String START = "startFrom";
	
	public static final String TRIPLE_COUNTING = "triple-counting";
	public static final String F_LIST = "giraph-f-list"; 
	public static final String FILE_PATTERN = "part-r-*";
	
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
    }
	
	/**
	 * Start step 1, counting of individual support 
	 */
	public static void startParallelCounting(GiraphConfiguration conf) 
			throws IOException, InterruptedException, ClassNotFoundException {
	    conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    	    
	    Job job = new Job(conf, "GiraphProjection: Counting");
	    job.setJarByClass(GiraphProjection.class); 

	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(conf.get(IN)));
	    Path output = new Path(conf.get(TMP)+"/"+TRIPLE_COUNTING);
	    
	    // delete existing tmp folder
	    ParallelProjection.delete(output, conf);
	    
	    FileOutputFormat.setOutputPath(job, output);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(TripleCounting.TripleCountingMapper.class);
	    job.setCombinerClass(TripleCounting.TripleCountingReducer.class);
	    job.setReducerClass(TripleCounting.TripleCountingReducer.class);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
	}
	
	/**
	 * Start step 2, generate f-list and save to HDFS
	 */
	public static void generateFList(
			String tmp,
			int minSupport,
			Configuration conf) throws IOException {
		// read
		FileSystem fs = FileSystem.get(conf);
		LongWritable key = new LongWritable(); 
		LongWritable value = new LongWritable(); 
		Map<Long, Long> freq = Maps.newHashMap();
		Path countResult = new Path(tmp+"/"+TRIPLE_COUNTING, FILE_PATTERN);
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
		
		// save to HDFS
		Path fListPath = new Path(tmp, F_LIST); 
		OutputStream out = fs.create(fListPath);
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(freq);
		oos.close();
		DistributedCache.addCacheFile(fListPath.toUri(), conf);		// add to dCache
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(OPTIONS, args);
		
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		giraphConf.setComputationClass(ProjectionComputation.class);
		giraphConf.setEdgeInputFormatClass(TripleEdgeInputFormat.class);
		giraphConf.setVertexOutputFormatClass(ProjectedGraphVertexOutputFormat.class);
		
		giraphConf.set(IN, cmd.getOptionValue(IN));
		giraphConf.set(OUT, cmd.getOptionValue(OUT));
		giraphConf.set(TMP, cmd.getOptionValue(TMP));
		giraphConf.set(SUPP, cmd.getOptionValue(SUPP));
		giraphConf.set(GROUP, cmd.getOptionValue(GROUP));
		
		int start = Integer.parseInt(cmd.getOptionValue(START, "1"));
		
		if (start <= 1) {
			startParallelCounting(giraphConf);
		}
		if (start <= 2) {
			generateFList(cmd.getOptionValue(TMP), 
					Integer.parseInt(cmd.getOptionValue(SUPP)), giraphConf);
		}
		
		int worker = Integer.parseInt(cmd.getOptionValue(WORKER));
		giraphConf.setWorkerConfiguration(worker, worker, 100.0f);
		giraphConf.set(ProjectionComputation.MIN_SUPPORT, cmd.getOptionValue(SUPP));
		giraphConf.setDoOutputDuringComputation(true);
		giraphConf.setEdgeInputFilterClass(EdgeBySupportFilter.class);

		GiraphJob job = new GiraphJob(giraphConf, "GiraphProjection: Projection "
                +"with "+cmd.getOptionValue(GROUP)+" groups");

		GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(cmd.getOptionValue(IN)));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue(OUT)));

		return job.run(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GiraphProjection(), args);
	}
}