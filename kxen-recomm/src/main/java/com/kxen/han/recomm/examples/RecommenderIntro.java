package com.kxen.han.recomm.examples;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;

class RecommenderIntro {
	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File("/home/port/datasets/ml-100k/ua.base"));
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(100,
				similarity, model);
		Recommender recommender = new GenericUserBasedRecommender(model,
				neighborhood, similarity);
		List<RecommendedItem> recommendations = recommender.recommend(1, 20);
		for (RecommendedItem recommendation : recommendations) {
			System.out.println(recommendation);
		}
	}
}