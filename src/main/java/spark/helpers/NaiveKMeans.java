
/*
 * KMeans.java ; Cluster.java ; Point.java
 *
 * Solution implemented by DataOnFocus
 * www.dataonfocus.com
 * 2015
 *
 */
package spark.helpers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;


public class NaiveKMeans implements Serializable {

    //Number of Clusters. This metric should be related to the number of points
    private int NUM_CLUSTERS;
    //Number of iterations
    private final int NUM_ITERATIONS;
    // Max value of trend
    private double MAX_VALUE;
    //Min and Max X and Y
    private double MIN_VALUE;
    // List of crentroids
    //private JavaPairRDD<Integer, Double> centroids;
    private List<Double> centroids;

    private JavaRDD<Iterable<Tuple2<Double, String>>> rddInput;
    private JavaPairRDD<Integer, Tuple2<Double, String>> points;
    private JavaPairRDD<String, Integer> rddOutClusters;

    public NaiveKMeans(JavaRDD<Iterable<Tuple2<Double, String>>> input, int clusters, int iterations) {
        this.rddInput = input;
        this.NUM_CLUSTERS = clusters;
        this.NUM_ITERATIONS = iterations;

        init();
    }

    /**
     * TODO: Implementare con gli RDD
     */
    private double findMax() {
        double max = 0;
        for (Iterable<Tuple2<Double, String>> t : rddInput.collect()) {
            for (Tuple2<Double, String> i : t) {
                if (i._1() > max) {
                    max = i._1();
                }
            }

        }

        System.out.println("Max Value naive " + max);
        return max;
    }


    /**
     * TODO: Implementare con gli RDD
     */
    private double findMin() {
        double min = MAX_VALUE;
        for (Iterable<Tuple2<Double, String>> t : rddInput.collect()) {
            for (Tuple2<Double, String> i : t) {
                if (i._1() < min) {
                    min = i._1();
                }
            }

        }

        System.out.println("Min Value naive " + min);
        return min;
    }


    //Initializes the process
    private void init() {

        MAX_VALUE = findMax();
        MIN_VALUE = findMin();

        //Set Random Centroids
        centroids = new ArrayList<>();
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            double centroid = createRandomPoint(MIN_VALUE, MAX_VALUE);
            centroids.add(centroid);
        }


        start();

    }


    //The process to calculate the K Means, with iterating method.
    private void start() {
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one. 
        while (iteration < NUM_ITERATIONS) {

            //Assign points to the closer cluster
            assignCluster();
            //Calculate new centroids.
            calculateCentroids();
            //Increment Iteration
            iteration++;

        }
    }


    private void assignCluster() {
        points = rddInput.flatMapToPair((PairFlatMapFunction<Iterable<Tuple2<Double, String>>, Integer, Tuple2<Double, String>>) tuple2s -> {
            double max = MAX_VALUE;
            double min = MIN_VALUE;
            int cluster;
            double distance;

            ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();

            for (Tuple2<Double, String> t : tuple2s) {
                //min = distance(centroids.get(0), t._1());
                min = MAX_VALUE;
                cluster = 0;
                for (int i = 0; i < NUM_CLUSTERS; i++) {
                    distance = distance(centroids.get(i), t._1());
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

        // Mi mappo tutti i trend per ogni cluster
        JavaPairRDD<Integer, Double> rddClusterTrends = points.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1()));
        // Faccio la somma di tutti i trend per ogni cluster
        JavaPairRDD<Integer, Double> rddTrendSum = rddClusterTrends.reduceByKey(Double::sum);


        // Creo un RDD appoggio per contare quanti sono i punti per ogni cluster
        JavaPairRDD<Integer, Double> rddCountPointsPerCluster = rddClusterTrends
                        .mapToPair(x -> new Tuple2<>(x._1(), 1.0))
                        .reduceByKey(Double::sum);

        JavaPairRDD<Integer, Tuple2<Double, Double>> join_fox0 = rddTrendSum.join(rddCountPointsPerCluster);

        //RDD con chiave il cluster e valore il nuovo centroide
        JavaPairRDD<Integer, Double> centroide_nuovo_final = join_fox0.mapToPair(x -> new Tuple2<>(x._1(), ((x._2()._1())/x._2()._2())));

        //Aggiorna Array List di centroidi con i nuovi
        for (Tuple2<Integer, Double> p : centroide_nuovo_final.distinct().collect()) {
            for (int i = 0; i < centroids.size(); i++)
                if (p._1() == i) {
                    centroids.set(i, p._2());
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


    public JavaPairRDD<Integer, Tuple2<Double, String>> getClusters() {
        return points;
    }

    public void plotCluster() {
        for (Tuple2<Integer, Tuple2<Double, String>> p : points.collect()) {
            System.out.println(p);
        }
    }

    public List<Double> getCentroids() {
        return centroids;
    }

}