package com.kxen.han.projection.pfpg;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
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
	
	static public final String IN = "i";
	static public final String OUT = "o";
	static public final String TMP = "tmp";
	static public final String GROUP = "g";
	static public final String SUPP = "s";
	static public final String START = "startFrom";
	static public final String FPG = "useObjectFPG";
	
	private static Options OPTIONS;
	
	static {
		OPTIONS = new Options();
		OPTIONS.addOption(IN, "input", true, "Input Path");
		OPTIONS.addOption(OUT, "output", true, "Output Path");
		OPTIONS.addOption(TMP, "tempDir", true, "Temp Directory");
		OPTIONS.addOption(GROUP, "numGroup", true, "Number of Groups");
		OPTIONS.addOption(SUPP, "minSupport", true, "Minimum Support Threshold");
		OPTIONS.addOption(START, true, "Input Path");
		OPTIONS.addOption(FPG, false, "Input Path");
	}

	/**
	 * Entry point for parallel bipartite graph projection
	 */
	public int run(String[] arg0) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(OPTIONS, arg0);
		
		Configuration conf = new Configuration();
		
		conf.set(ParallelProjection.NUM_GROUP, cmd.getOptionValue(GROUP));
		conf.set(IN, cmd.getOptionValue(IN));
		conf.set(OUT, cmd.getOptionValue(OUT));
		conf.set(TMP, cmd.getOptionValue(TMP));
		conf.set(ParallelProjection.MIN_SUPPORT, cmd.getOptionValue(SUPP, "2"));
		conf.set(START, cmd.getOptionValue(START, "1"));
		conf.setBoolean(FPG, cmd.hasOption(FPG));
		ParallelProjection.runProjection(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ParallelProjectionDriver(), args);
	}
}
