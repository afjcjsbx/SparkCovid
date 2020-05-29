package com.afjcjsbx.sabdcovid.spark.helpers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;


public class NaiveKMeans implements Serializable {

    // Numero di clusers, ovviamente dovrà essere un numero sempre
    // minore al numero di punti che andiamo a classificare
    private int NUM_CLUSTERS;
    // Numero di iterazioni da far eseguire all'algoritmo
    private final int NUM_ITERATIONS;
    // Valore massimo del punto
    private double MAX_VALUE;
    // Valore minimo del punto
    private double MIN_VALUE;
    // Lista dei centroidi
    private List<Double> centroids;
    // RDD'S di ausilio
    private JavaRDD<Iterable<Tuple2<Double, String>>> rddInput;
    private JavaPairRDD<Integer, Tuple2<Double, String>> points;

    public NaiveKMeans(JavaRDD<Iterable<Tuple2<Double, String>>> input, int clusters, int iterations) {
        this.rddInput = input;
        this.NUM_CLUSTERS = clusters;
        this.NUM_ITERATIONS = iterations;

        init();
    }

    /**
     * TODO: Implementare con gli RDD
     */
    // Trovo il valore massimo tra tutti i punti da classificare
    private double findMax() {
        double max = 0;
        for (Iterable<Tuple2<Double, String>> t : rddInput.collect()) {
            for (Tuple2<Double, String> i : t) {
                if (i._1() > max) {
                    max = i._1();
                }
            }
        }

        return max;
    }

    /**
     * TODO: Implementare con gli RDD
     */
    // Trovo il valore minimo tra tutti i punti da classificare
    private double findMin() {
        double min = MAX_VALUE;
        for (Iterable<Tuple2<Double, String>> t : rddInput.collect()) {
            for (Tuple2<Double, String> i : t) {
                if (i._1() < min) {
                    min = i._1();
                }
            }
        }

        return min;
    }


    // Inizializzazione del processo
    private void init() {

        // Calcolo minimo e massimo
        MAX_VALUE = findMax();
        MIN_VALUE = findMin();

        // Produco dei centroidi con coordinate random entro il range [MII, MAX]
        centroids = new ArrayList<>();
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            double centroid = createRandomPoint(MIN_VALUE, MAX_VALUE);
            centroids.add(centroid);
        }
        // Inizio il training
        start();
    }


    // Processo per calcolare il KMeans con il metodo iterativo
    private void start() {
        int iteration = 0;

        // Aggiunge i nuovi dati ricalcolando i centroidi ogni volta.
        while (iteration < NUM_ITERATIONS) {
            // Assegno i punti al cluster più vicino
            assignCluster();
            // Calcolo i nuovi centroidi
            calculateCentroids();
            // Incremento l'iterazione
            iteration++;
        }
    }


    // Assegno il cluster ai punti
    private void assignCluster() {

        points = rddInput.flatMapToPair((PairFlatMapFunction<Iterable<Tuple2<Double, String>>, Integer, Tuple2<Double, String>>) tuples -> {
            double min;
            int cluster;
            double distance;

            // ArrayList di appoggio <cluster <trend, stato>>
            ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();

            // Itero per tutti i punti
            for (Tuple2<Double, String> tuple : tuples) {
                min = MAX_VALUE;
                cluster = 0;
                // Per ogni cluster mi calcolo la distanza e se è inferiore
                // al massimo assegno il punto a quel cluster
                for (int i = 0; i < NUM_CLUSTERS; i++) {
                    // Calcolo la distanza dal centroie
                    distance = distance(centroids.get(i), tuple._1());
                    if (distance < min) {
                        min = distance;
                        cluster = i;
                    }
                }

                // Assegno il punto al cluster
                res.add(new Tuple2<>(cluster, tuple));
            }
            return res.iterator();
        });
    }



    private void calculateCentroids() {

        // Mi mappo tutti i trend per ogni cluster
        JavaPairRDD<Integer, Double> rddClusterTrends = points.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1()));
        // Faccio la somma di tutti i trend per ogni cluster
        JavaPairRDD<Integer, Double> rddTrendSum = rddClusterTrends.reduceByKey(Double::sum);

        // Creo un RDD di appoggio per contare quanti sono i punti per ogni cluster
        JavaPairRDD<Integer, Double> rddCountPointsPerCluster = rddClusterTrends
                        .mapToPair(x -> new Tuple2<>(x._1(), 1.0))
                        .reduceByKey(Double::sum);

        // Unisco i due RDD's (cluster, (trend, punto)
        JavaPairRDD<Integer, Tuple2<Double, Double>> rddAusiliar = rddTrendSum.join(rddCountPointsPerCluster);

        //RDD con chiave il cluster e valore il nuovo centroide
        JavaPairRDD<Integer, Double> rddClusterValue = rddAusiliar.mapToPair(x -> new Tuple2<>(x._1(), ((x._2()._1())/x._2()._2())));

        //Aggiorna Array List di centroidi con i nuovi
        for (Tuple2<Integer, Double> cluster : rddClusterValue.distinct().collect()) {
            for (int i = 0; i < centroids.size(); i++)
                if (cluster._1() == i) {
                    centroids.set(i, cluster._2());
                }
        }

    }

    // Crea un punto random compreso nell'intervallo [MIN, MAX]
    protected static double createRandomPoint(double min, double max) {
        Random r = new Random();
        return min + (max - min) * r.nextDouble();
    }

    // Calcola la distanza tra due punti in uno spazio monodimensionale
    protected static double distance(double p, double centroid) {
        return Math.sqrt(Math.pow((centroid - p), 2));
    }


    public JavaPairRDD<Integer, Tuple2<Double, String>> getClusters() {
        return points;
    }

    public List<Double> getCentroids() {
        return centroids;
    }

}