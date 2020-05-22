
/*
 * KMeans.java ; Cluster.java ; Point.java
 *
 * Solution implemented by DataOnFocus
 * www.dataonfocus.com
 * 2015
 *
 */
package spark.helpers;

import model.Covid2Data;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;


public class NaiveKMeans implements Serializable {

    //Number of Clusters. This metric should be related to the number of points
    private int NUM_CLUSTERS;
    //Number of iterations
    private int NUM_ITERATIONS;
    //Number of Points
    private int NUM_POINTS = 15;
    // Max value of trend
    private double MAX_VALUE;
    //Min and Max X and Y
    private static final int MIN_VALUE = 0;
    // List of crentroids
    //private JavaPairRDD<Integer, Double> centroids;
    private List<Double> centroids;

    private JavaPairRDD<Integer, Tuple2<Double, String>> points;
    private JavaRDD<Iterable<Tuple2<Double, String>>> rddin;
    private JavaPairRDD<Integer, String> rddOutClusters;

    public NaiveKMeans(JavaRDD<Iterable<Tuple2<Double, String>>> points, int clusters, int iterations) {
        this.rddin = points;
        this.NUM_CLUSTERS = clusters;
        this.NUM_ITERATIONS = iterations;

        init();
    }


    private double findMax(){
        List<Iterable<Tuple2<Double, String>>> i = rddin.collect();
        double max = 0;
        for (Iterable<Tuple2<Double, String>> t : i) {
            for (Tuple2<Double, String> c : t) {

                if (c._1() > max) {
                    max = c._1();
                }
            }
        }
        System.out.println("Max Value naive " + max);
        return max;
    }

    //Initializes the process
    private void init() {

        MAX_VALUE = findMax();

        //List<Tuple2<Integer, Double>> centroidsist = new ArrayList<>();
        //Set Random Centroids
        centroids = new ArrayList<>();
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            double centroid = createRandomPoint(MIN_VALUE, MAX_VALUE);
            centroids.add(centroid);
        }

       // JavaRDD<Tuple2<Integer, Double>> c = sparkContext.parallelize(centroidsist);

        //centroids = c.mapToPair(x-> new Tuple2<>(x._1(),x._2()));
    }


    //The process to calculate the K Means, with iterating method.
    public void start() {
        boolean finish = false;
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one. 
        while (iteration < NUM_ITERATIONS) {

            //List<Tuple2<Integer, Double>> lastCentroids = centroids.collect();
            //Assign points to the closer cluster
            assignCluster();

            //Calculate new centroids.
            calculateCentroids();

            iteration++;

           // List<Tuple2<Integer, Double>> currentCentroids = centroids.collect();

            //Calculates total distance between new and old Centroids
            /*
            double distance = 0;
            for (int i = 0; i < lastCentroids.size(); i++) {
                distance += distance(lastCentroids.get(i), currentCentroids.get(i));
            }

             */

            System.out.println("Iteration: #" + iteration);

        }
    }


    private void assignCluster() {
        points = rddin.flatMapToPair((PairFlatMapFunction<Iterable<Tuple2<Double, String>>, Integer, Tuple2<Double, String>>) tuple2s -> {
            double max = Double.MAX_VALUE;
            double min;
            int cluster;
            double distance;

            ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();

            for (Tuple2<Double, String> t : tuple2s) {
                min = max;
                cluster = 0;
                for (int i = 0; i < NUM_CLUSTERS; i++) {
                    distance = distance(centroids.get(0), t._1());
                    if (distance < min) {
                        min = distance;
                        cluster = i;
                    }
                }
                res.add(new Tuple2<>(cluster, t));

            }
            return res.iterator();
        });
    }


    private void calculateCentroids() {
        // Mi calcolo i nnuovi centroidi e li vado ad aggiornare
        for (Tuple2<Integer, Tuple2<Double, String>> p : points.distinct().collect()) {
            for (int i = 0; i < centroids.size(); i++)
                if (p._1() == i) {
                    centroids.set(i, p._2()._1());
                }
        }

    }


    //Creates random point
    protected static double createRandomPoint(double min, double max) {
        Random r = new Random();
        return min + (max - min) * r.nextDouble();
    }


    //Calculates the distance between two points.
    protected static double distance(double p, double centroid) {
        return Math.sqrt(Math.pow((centroid - p), 2));
    }


    public void plotCluster() {
        for(Tuple2<Integer, Tuple2<Double, String>> p : points.collect()) {
            System.out.println(p);
        }
    }

    public void plotCentroids() {
        for(int i = 0; i < centroids.size(); i++) {
            System.out.println("Cluster: " + i + " centroid: " + centroids.get(i));
        }
    }

}