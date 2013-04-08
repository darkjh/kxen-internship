package com.kxen.han.recomm.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

public class SimpleSVDRecomm {
	static private String DATA_100K = "/home/port/datasets/ml-100k/bool";
	static private String DATA_KAR = "/home/port/datasets/KAR/mahout/transaction";
	static private String DATA_1M = "/home/port/datasets/ml-1m/r.bool";
	static private String DATA_MS = "/home/port/datasets/million-song/test_triples";
	static private String FILE = DATA_MS;

	static private RecommenderBuilder matrixFacoriaztionBasedRecommBuilder = new RecommenderBuilder() {

		public Recommender buildRecommender(DataModel model)
				throws TasteException {
			// Factorizer factorizer1 = new SVDPlusPlusFactorizer(model, 10, 10);
			Factorizer factorizer2 = new ALSWRFactorizer(model, 20, 150, 10, true, 40);
			return new SVDRecommender(model, factorizer2);
		}
	};

	static public void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File(FILE));
		Recommender recomm = matrixFacoriaztionBasedRecommBuilder.buildRecommender(model);
		LongPrimitiveIterator userIter = model.getUserIDs();
		
		File file = new File("/home/port/outputs/million-songs/test-triples-recomm.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
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
}