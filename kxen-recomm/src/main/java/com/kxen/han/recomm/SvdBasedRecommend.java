package com.kxen.han.recomm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

public class SvdBasedRecommend {
	
	static private String IN = "input";
	static private String OUT = "output";
	static private String ITER = "iter";
	static private String LAMBDA = "lambda";
	static private String ALPHA = "alpha";
	static private String FEATURE = "feature";
	static private String IMP = "implicit";
		
	
	static private Options initOptions() {
		Options ops = new Options();
		
		Option inputPath = OptionBuilder.withArgName("inputPath").hasArg().create(IN);
		Option outputPath = OptionBuilder.withArgName("outputPath").hasArg().create(OUT);
		Option numIter = OptionBuilder.withArgName("numIter").hasArg().create(ITER);
		Option lambda = OptionBuilder.withArgName("lambda").hasArg().create(LAMBDA);
		Option alpha = OptionBuilder.withArgName("alpha").hasArg().create(ALPHA);
		Option feature = OptionBuilder.withArgName("numFeature").hasArg().create(FEATURE);
		Option implicit = new Option(IMP, "use implicit feedback");
		
		ops.addOption(inputPath);
		ops.addOption(outputPath);
		ops.addOption(numIter);
		ops.addOption(lambda);
		ops.addOption(alpha);
		ops.addOption(feature);
		ops.addOption(implicit);
		
		return ops;
	}
	
	static private Recommender buildLatentFactorBasedRecommender(
			DataModel model, int numFeatures, int numIteration,
			boolean implicit, double lambda, double alpha)
			throws TasteException {

		Factorizer factorizer = null;
		if (implicit) {
			factorizer = new ALSWRFactorizer(model, numFeatures, lambda,
					numIteration, true, alpha);
		} else {
			factorizer = new ALSWRFactorizer(model, numFeatures, lambda,
					numIteration);
		}

		return new SVDRecommender(model, factorizer);
	}
	
	/** 
	 * Recommend for every user in datamodel
	 */
	static private void recommend(DataModel model, Recommender recomm, File output) 
			throws Exception {
		LongPrimitiveIterator userIter = model.getUserIDs();
		
		if (!output.exists()) {
			output.createNewFile();
		}
		
		FileWriter fw = new FileWriter(output.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		while (userIter.hasNext()) {
			long userID = userIter.nextLong();
			List<RecommendedItem> recomms = recomm.recommend(userID, 500);
			
			bw.write(userID + "\t");
			for (RecommendedItem item: recomms) {
				bw.write(item.getItemID() + " ");
			}
			bw.newLine();
		}
		bw.close();
	}
	
	static public void main(String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		CommandLine line = parser.parse(initOptions(), args);
		
		File input = new File(line.getOptionValue(IN));
		File output = new File(line.getOptionValue(OUT));
		int iter = Integer.parseInt(line.getOptionValue(ITER));
		int feature = Integer.parseInt(line.getOptionValue(FEATURE));
		double lambda = Double.parseDouble(line.getOptionValue(LAMBDA));
		double alpha = Double.parseDouble(line.getOptionValue(ALPHA));
		
		DataModel model = new FileDataModel(input);
		Recommender recomm = buildLatentFactorBasedRecommender(model, feature, iter, 
				line.hasOption(IMP), lambda, alpha);
		recommend(model, recomm, output);
	}
}