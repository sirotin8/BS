package de.hshannover.vis.mahout;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class FirstRecommender{

public static void main(String args[]) {
if (args.length != 1) {
System.out
.println("Usage: FirstRecommender [n-dimensional Points]");
System.out.println("current args: " + args);

System.exit(1);
}

DataModel model;
try {
model = new FileDataModel(new File(args[0]));

UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1,
similarity, model);

UserBasedRecommender recommender = new GenericUserBasedRecommender(
model, neighborhood, similarity);

List<RecommendedItem> recommendations = recommender.recommend(2, 3);
for (RecommendedItem recommendation : recommendations) {
System.out.println(recommendation);
}

} catch (IOException | TasteException e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
}

}

