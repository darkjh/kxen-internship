package com.kxen.han.projection.giraph;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.kxen.han.projection.pfpg.ParallelProjection;

public class GiraphProjection 
extends Configured implements Tool {
	
	/** cli interface set-up */
	static public final String IN = "i";
	static public final String OUT = "o";
	static public final String SUPP = "s";
	static public final String TMP = "tmp";
	static public final String WORKER = "w";
	
	public static final String TRIPLE_COUNTING = "triple-counting";
	
	private static Options OPTIONS;
	
	static {
		OPTIONS = new Options();
		OPTIONS.addOption(IN, "input", true, "Input Path");
		OPTIONS.addOption(OUT, "output", true, "Output Path");
		OPTIONS.addOption(TMP, "tempDir", true, "Temp Directory");
		OPTIONS.addOption(SUPP, "minSupport", true, "Minimum Support Threshold");
		OPTIONS.addOption(WORKER, "worker", true, "Number of Wokers To Use");
	}
	
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
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(TripleCounting.TripleCountingMapper.class);
	    job.setCombinerClass(TripleCounting.TripleCountingReducer.class);
	    job.setReducerClass(TripleCounting.TripleCountingReducer.class);
	    
	    if (!job.waitForCompletion(false)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
	}
	
	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(OPTIONS, args);
		
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		giraphConf.setComputationClass(ProjectionComputation.class);
		giraphConf.setEdgeInputFormatClass(TripleEdgeInputFormat.class);
		giraphConf.setVertexOutputFormatClass(ProjectedGraphOutputFormat.class);

		startParallelCounting(giraphConf);
		ParallelProjection.generateFList(cmd.getOptionValue(TMP)+"/"+TRIPLE_COUNTING, 
				Integer.parseInt(cmd.getOptionValue(SUPP)), giraphConf);
		
		int worker = Integer.parseInt(cmd.getOptionValue(WORKER));
		giraphConf.setWorkerConfiguration(worker, worker, 100.0f);
		giraphConf.set(ProjectionComputation.MIN_SUPPORT, cmd.getOptionValue(SUPP));
		GiraphJob job = new GiraphJob(giraphConf, "GiraphProjection: Projection");
		GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(cmd.getOptionValue(IN)));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(cmd.getOptionValue(OUT)));
		
		return job.run(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GiraphProjection(), args);
	}
}