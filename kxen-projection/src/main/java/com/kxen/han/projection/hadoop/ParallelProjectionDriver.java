package com.kxen.han.projection.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A driver class that allows using hadoop cli tool to run parallel bipartite
 * graph projection job
 * 
 * @author Han JU
 *
 */
public class ParallelProjectionDriver extends Configured implements Tool {
		
	static private final String IN = "input";
	static private final String OUT = "output";
	static private final String TMP = "tempDir";
	static private final String GROUP = "numGroup";
	static private final String SUPP = "minSupport";
	static private final String START = "startFrom";
	
	private static Options initOptions() {
		Options ops = new Options();
		
		Option inputPath = OptionBuilder.withArgName("input").hasArg().create(IN);
		Option outputPath = OptionBuilder.withArgName("output").hasArg().create(OUT);
		Option numGroup = OptionBuilder.withArgName("Group").hasArg().create(GROUP);
		Option tmpPath = OptionBuilder.withArgName("temp").hasArg().create(TMP);
		Option minSupport = OptionBuilder.withArgName("minSupport").hasArg().create(SUPP);
		Option startFrom = OptionBuilder.withArgName("start").hasArg().create(START);
		
		ops.addOption(inputPath);
		ops.addOption(outputPath);
		ops.addOption(numGroup);
		ops.addOption(tmpPath);
		ops.addOption(minSupport);
		ops.addOption(startFrom);
		
		return ops;
	}

	/**
	 * Entry point for parallel bipartite graph projection
	 */
	public int run(String[] arg0) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cli = parser.parse(initOptions(), arg0);
		
		String input = cli.getOptionValue(IN);
		String output = cli.getOptionValue(OUT);
		String tmp = cli.getOptionValue(TMP);
		int groupNum = Integer.parseInt(cli.getOptionValue(GROUP));
		int minSupport = Integer.parseInt(cli.getOptionValue(SUPP, "2"));
		int startFrom = Integer.parseInt(cli.getOptionValue(START, "1"));
		ParallelProjection.runProjection(input, output, tmp, 
				minSupport, groupNum, startFrom);		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ParallelProjectionDriver(), args);
	}
}
