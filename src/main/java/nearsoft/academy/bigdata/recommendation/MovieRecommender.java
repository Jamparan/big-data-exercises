package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;


public class MovieRecommender {

    static String filePath;
    static String user;
    static long totalReviews = 0;
    static HashBiMap<String, Integer> users;
    static HashBiMap<String, Integer> products;
    static List<String> recommendations;
    static FileInputStream fstream;
    static BufferedReader br;
    static private GenericUserBasedRecommender recommender;


    public MovieRecommender(String str) {
        try {
            str = "movies.txt.gz";
            File tempFile = new File("file.csv");
            if (tempFile.exists()) {
                tempFile.delete();
            }
            tempFile.createNewFile();
            InputStream fileStream = new FileInputStream(str);
            InputStream gzipStream = new GZIPInputStream(fileStream);
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            BufferedReader buffered = new BufferedReader(decoder);

            Writer writer = new BufferedWriter(new FileWriter(tempFile));

            String line;
            String user = null;
            String product = null;
            String[] tempValues;
            users = HashBiMap.create();
            products = HashBiMap.create();
            String score = "0";
            while ((line = buffered.readLine()) != null) {

                if (line.startsWith("product/productId:")) {
                    tempValues = line.split(": ");
                    product = tempValues[1];
                    line = buffered.readLine();
                }
                if (line.startsWith("review/userId:")) {
                    tempValues = line.split(": ");
                    user = tempValues[1];
                    buffered.readLine();
                    buffered.readLine();
                    line = buffered.readLine();
                }

                if (!products.containsKey(product)) {
                    products.put(product, products.size() + 1);
                }

                if (!users.containsKey(user)) {
                    users.put(user, users.size() + 1);
                }
                if (line.startsWith("review/score:")) {
                    tempValues = line.split(": ");
                    score = tempValues[1];
                    totalReviews++;
                    writer.append(String.valueOf(users.get(user)));
                    writer.append(",");
                    writer.append(String.valueOf(products.get(product)));
                    writer.append(",");
                    writer.append(score);
                    writer.append("\n");
                    buffered.readLine();
                    buffered.readLine();
                    buffered.readLine();
                }

            }
            writer.close();
            DataModel model = new FileDataModel(new File("file.csv"));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
            System.out.println(totalReviews);
        } catch (Exception e) {

        }

    }

    public List<String> getRecommendationsForUser(String userName) {
        List<RecommendedItem> recommendations = null;
        List<String> recommendedMovies = null;
        try {
            recommendations = recommender.recommend(users.get(userName), 3);
            recommendedMovies = new ArrayList<String>();
            BiMap<Integer, String> productsByName = products.inverse();
            for (RecommendedItem recommendation : recommendations) {
                String movieName = productsByName.get((int) recommendation.getItemID());
                recommendedMovies.add(movieName);
            }


            return recommendedMovies;
        } catch (TasteException e) {
            e.fillInStackTrace();
        }
        return recommendedMovies;
    }

    public static void main(String[] args) {
    }


    public static long getTotalReviews() {
        return totalReviews;
    }

    public static long getTotalProducts() {
        return products.size();
    }

    public static long getTotalUsers() {
        return users.size();
    }
}
