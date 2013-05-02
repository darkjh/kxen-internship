package com.kxen.han.projection.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Convert a user-item-count format file into transaction format
 * 
 * @author Han JU
 *
 */
public class PreProcessor extends Configured implements Tool {
	
	static private final String IN = "input";
	static private final String OUT = "output";
		
	private static Options initOptions() {
		Options ops = new Options();
		
		Option inputPath = OptionBuilder.withArgName("input").hasArg().create(IN);
		Option outputPath = OptionBuilder.withArgName("output").hasArg().create(OUT);
		
		ops.addOption(inputPath);
		ops.addOption(outputPath);
		
		return ops;
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cli = parser.parse(initOptions(), arg0);
		
		String in = cli.getOptionValue(IN);
		String out = cli.getOptionValue(OUT);
		
		Configuration conf = new Configuration();
		conf.set("mapred.compress.map.output", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");

		Job job = new Job(conf, "Pre processing with input:" + in);
		job.setJarByClass(PreProcessor.class);
		
	    // setting input and output path
	    FileInputFormat.addInputPath(job, new Path(in));
	    Path output = new Path(out);
	    
	    
	    FileOutputFormat.setOutputPath(job, output);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TransactionWritable.class);
	    job.setMapperClass(PreProcessingMapper.class);
	    job.setReducerClass(PreProcessingReducer.class);
	    
	    if (!job.waitForCompletion(true)) {
	    	throw new IllegalStateException("Job failed ...");
	    }
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PreProcessor(), args);
	}
}
