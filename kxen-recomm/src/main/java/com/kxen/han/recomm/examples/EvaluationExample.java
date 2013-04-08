package com.kxen.han.recomm.examples;

import java.io.File;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

class EvaluationExample {
	static private String DATA_PREFIX = "/home/port/datasets/ml-100k/";
	static private String DATA_1M = "/home/port/datasets/ml-1m/r.bool";
	static private String DATA_MS = "/home/port/datasets/million-song/test_triples";
	static private String FILE = DATA_MS;
	
	// build a recommender for evaluation
	static private RecommenderBuilder userBasedRecommBuilder = new RecommenderBuilder() {
		
		public Recommender buildRecommender(DataModel model) throws TasteException {
			UserSimilarity similarity = new LogLikelihoodSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(20, similarity, model);
			return new GenericBooleanPrefUserBasedRecommender(model, neighborhood, similarity);
		}
	};
	
	static private RecommenderBuilder itemBasedRecommBuilder = new RecommenderBuilder() {
		
		public Recommender buildRecommender(DataModel model) throws TasteException {
			ItemSimilarity similarity = new LogLikelihoodSimilarity(model);
			return new GenericBooleanPrefItemBasedRecommender(model, similarity);
		}
	};
	
	static private RecommenderBuilder matrixFacoriaztionBasedRecommBuilder = new RecommenderBuilder() {
		
		public Recommender buildRecommender(DataModel model) throws TasteException {
			// Factorizer factorizer1 = new SVDPlusPlusFactorizer(model, 10, 10);
			Factorizer factorizer2 = new ALSWRFactorizer(model, 20, 150, 10, true, 40);
			return new SVDRecommender(model, factorizer2);
		}
	};
	
	static private DataModelBuilder boolModelBuilder = new DataModelBuilder() {
		public DataModel buildDataModel(
				FastByIDMap<PreferenceArray> trainingData) {
			return new GenericBooleanPrefDataModel(
					GenericBooleanPrefDataModel.toDataMap(trainingData));
		}
	};
	
	private EvaluationExample() {}
	
	private static void PredictionBasedBoolEvaluate(RecommenderBuilder builder, DataModelBuilder dataBuilder) throws Exception {
		// used only in examples and unit tests, give always same randomness
		// RandomUtils.useTestSeed();
//		DataModel model = new GenericBooleanPrefDataModel( 
//        		GenericBooleanPrefDataModel.toDataMap(
//        				new FileDataModel(new File(FILE))));
		DataModel model = new FileDataModel(new File(FILE));
		// RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		RecommenderEvaluator rmse = new RMSRecommenderEvaluator();
		
		// double score = evaluator.evaluate(builder, dataBuilder, model, 0.7, 0.5);
		double rmseScore = rmse.evaluate(builder, dataBuilder, model, 0.7, 0.5);
		// System.out.println("Average Diff: " + score);
		System.out.println("RMSE:         " + rmseScore);
	}
	
	private static void IRBasedBoolEvaluate(RecommenderBuilder builder, DataModelBuilder dataBuilder) throws Exception {
		// RandomUtils.useTestSeed();
		DataModel model = new GenericBooleanPrefDataModel( 
        		GenericBooleanPrefDataModel.toDataMap(
        				new FileDataModel(new File(FILE))));
		RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = evaluator.evaluate(builder, dataBuilder,
				model, null, 10,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		System.out.println("Precision:  " + stats.getPrecision());
		// System.out.println("Recall:     " + stats.getRecall());
		// System.out.println("F1 measure: " + stats.getF1Measure());
	}
	
	public static void main(String[] args) throws Exception {
		PredictionBasedBoolEvaluate(matrixFacoriaztionBasedRecommBuilder, null);
		// IRBasedBoolEvaluate(matrixFacoriaztionBasedRecommBuilder, null);
	}
}