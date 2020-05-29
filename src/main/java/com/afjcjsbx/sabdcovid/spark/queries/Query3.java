package com.afjcjsbx.sabdcovid.spark.queries;

import com.afjcjsbx.sabdcovid.model.Config;
import com.afjcjsbx.sabdcovid.model.Covid2Data;
import com.afjcjsbx.sabdcovid.model.RedisConnection;
import com.afjcjsbx.sabdcovid.spark.helpers.Common;
import com.afjcjsbx.sabdcovid.spark.helpers.NaiveKMeans;
import com.afjcjsbx.sabdcovid.utils.DataParser;
import com.afjcjsbx.sabdcovid.utils.LinearRegression;
import com.afjcjsbx.sabdcovid.utils.RegionParser;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import redis.clients.jedis.exceptions.JedisConnectionException;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Query3 implements IQuery {

    private final JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid2Data> rddIn;
    @Getter
    private JavaPairRDD<Integer, Tuple2<String, Integer>> rddOutMlib;
    @Getter
    private JavaPairRDD<Integer, Tuple2<String, Integer>> rddOutNaive;
    @Getter
    private JavaPairRDD<String, String> rddPairCountryContinent;


    public Query3(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * Caricamento dei dati dal data store HDFS
     */
    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(Config.PATH_DATASET_2);
        String header = input.first();

        // RDD conenente le righe del CSV
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        // Parso i dati
        rddIn = rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header)).cache();


        // Creo un RDD con tutti gli stati
        JavaRDD<String> rddCountryContinent = sparkContext.textFile(Config.PATH_COUNTRY_CONTINENT);
        String headerRegion = rddCountryContinent.first();
        rddCountryContinent = rddCountryContinent.filter(x -> !x.equals(headerRegion));
        rddPairCountryContinent = rddCountryContinent
                .mapToPair(x -> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountry(), RegionParser.parseCSVRegion(x).getContinent()));

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files in Query 3: %s ms\n", (fParseFile - iParseFile));
    }

    /**
     * Esecuzione della query 3.
     * <p>
     * QUERY 3:
     * Per ogni mese usare l'algoritmo di clustering del K-means con (K=4) per
     * identificare gli stati che appartengono ad ogni cluster rispetto al trend
     * dei casi confermati.
     * Nota: Considerare solo i maggiori 50 stati colpiti ogni mese, applicare
     * l'algoritmo del k-means mese per mese per identificare gli stati che hanno
     * un trend simile di casi confermati durante quel mese. Comparare le performance
     * dell'implementazione naive dell'algoritmo con quelle delle l'algoritmo
     * implementato nella libreria di Spark Mlib o Apache Mahout
     */
    @Override
    public void execute() {
        long initialTime = System.currentTimeMillis();

        // Creo un RDD composto da una String che rappresenta il nome dello stato e una lista di interi in
        // cui ci sono i casi totali relativi giorno per giorno
        JavaPairRDD<String, List<Integer>> rddStateCases = rddIn.mapToPair(x -> new Tuple2<>(x.getState(), x.getCases()));


        // Creo un RDD composto da una stringa che rappresenta il nome dello stato e un ArrayList
        JavaPairRDD<String, List<Integer>> rddContinentDayByDay = rddStateCases
                .reduceByKey((Function2<List<Integer>, List<Integer>, List<Integer>>) (arr1, arr2) -> {
                    List<Integer> sum = new ArrayList<>();
                    for (int z = 0; z < arr1.size(); z++)
                        sum.add(arr1.get(z) + arr2.get(z));
                    return sum;
                });


        // Da implementare nel load
        JavaRDD<String> input = sparkContext.textFile(Config.PATH_DATASET_2);
        String header = input.first();
        String[] cols = header.split(",");
        ArrayList<String> dates = new ArrayList<>(Arrays.asList(cols).subList(4, cols.length));
        ArrayList<Integer> months = new ArrayList<>();
        for (String date : dates) {
            months.add(Common.getMonthFromDate(date));
        }


        // Creo un RDD che coterrà il numero dello stato, il mese e i casi relativi all'n-esimo giorno di quel mese
        JavaPairRDD<Tuple2<String, Integer>, Integer> rddComplete = rddContinentDayByDay
                .flatMapToPair((PairFlatMapFunction<Tuple2<String, List<Integer>>, Tuple2<String, Integer>, Integer>) listTuple2 -> {
                    List<Tuple2<Tuple2<String, Integer>, Integer>> result_flat = new ArrayList<>();

                    for (int i = 1; i < months.size(); i++) {
                        Tuple2<Tuple2<String, Integer>, Integer> temp = new Tuple2<>(
                                new Tuple2<>(listTuple2._1(), months.get(i)), listTuple2._2().get(i));
                        result_flat.add(temp);
                    }

                    return result_flat.iterator();
                });


        // Raggruppo per chiave : <Stato ,Mese> ottenendo un RDD <Stato ,Mese>, Iterable
        // dei nuovi casi del mese giorno per giorno
        JavaPairRDD<Tuple2<String, Integer>, Iterable<Integer>> rddTotalCasesInStateByMonth = rddComplete.groupByKey();


        // Colcoliamo il trend mese per mese per ogni stato ottenendo cos' un RDD che ha come
        // chiave il mese e come campo una tupla2 contenente come primo valore il Trend e come
        // secondo valore il nome dello stato
        JavaPairRDD<Integer, Tuple2<String, Double>> rddGroupedPerMonth = rddTotalCasesInStateByMonth.mapToPair((PairFunction<Tuple2<Tuple2<String, Integer>, Iterable<Integer>>, Integer, Tuple2<String, Double>>) input1 -> {

            String state_name = input1._1()._1();
            Integer month = input1._1()._2();
            List<Integer> casesPerMonth = new ArrayList<>();

            for (Integer value : input1._2()) {
                casesPerMonth.add(value);
            }

            double trendLineCoefficint = new LinearRegression(casesPerMonth).getCoefficient();
            return new Tuple2<>(month, new Tuple2<>(state_name, trendLineCoefficint));
        });


        // Raggrupiamo creando un rdd contenente per chiave i mesi e per valore un iterable con i trend
        // per ogni stato dei maggiori 50 precedentemente calcolati
        JavaPairRDD<Integer, Iterable<Tuple2<String, Double>>> rddResultGrouped = rddGroupedPerMonth.groupByKey();


        // Definiamo un arrayList contente le tuple del tipo <Numero Mese, Lista di Tuple<Trend, Nome stato>> corrispettivi al mese
        List<Tuple2<Integer, List<Tuple2<Double, String>>>> listTopStatesPerMonth = new ArrayList<>();

        // Aggiungo le tuple alla lista di liste precedentemente creata
        for (int i = 0; i < rddResultGrouped.countByKey().size(); i++) {

            int fI = i;
            JavaPairRDD<Integer, Iterable<Tuple2<String, Double>>> rddMonthsStates = rddResultGrouped.filter(x -> x._1().equals(fI));
            JavaPairRDD<Double, String> rddMonthsCoefficient = rddMonthsStates.
                    flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<String, Double>>>, Double, String>) input12 -> {

                        List<Tuple2<Double, String>> result = new ArrayList<>();
                        for (Tuple2<String, Double> tuple : input12._2()) {
                            result.add(new Tuple2<>(tuple._2(), tuple._1()));
                        }
                        return result.iterator();
                    });

            List<Tuple2<Double, String>> top = rddMonthsCoefficient.sortByKey(false).take(50);

            listTopStatesPerMonth.add(new Tuple2<>(fI, top));
        }


        // Trasformo in RDD la lista
        JavaRDD<Tuple2<Integer, List<Tuple2<Double, String>>>> input2 = sparkContext.parallelize(listTopStatesPerMonth);

        // Mappo il precedente RDD in un PairRdd così formato <mese, <coefficienteTrend, nomeStato>>
        JavaPairRDD<Integer, Tuple2<Double, String>> pairRddTopStatesPerMonth = input2.
                flatMapToPair((PairFlatMapFunction<Tuple2<Integer, List<Tuple2<Double, String>>>, Integer, Tuple2<Double, String>>) row -> {
                    List<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();
                    for (Tuple2<Double, String> tuple : row._2()) {
                        res.add(new Tuple2<>(row._1(), tuple));
                    }
                    return res.iterator();
                });


        // RDD contenente come chiave il mese come valore una lista di tuple contenente per ogni stato il rispettivo trend
        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> rddGoupedStatesPerMonth = pairRddTopStatesPerMonth.groupByKey();

        // Creo una lisa di tuple che mi serve come appoggio per i risultati
        List<Tuple2<Integer, Tuple2<String, Integer>>> listMlibResults = new ArrayList<>();

        long iSparkKmeans = System.currentTimeMillis();

        /**
         * Start MLib KMeans
         */
        // Itero per tutti i mesi e applico il KMeans mese per mese
        for (int month = 0; month < rddGoupedStatesPerMonth.keys().collect().size(); month++) {

            // Filtro il risultati per il mese corrente che sto valutando
            int finalMonth = month;

            // Aggiungo i coefficienti in un RDD di Vector per darli in pasto al KMeans
            JavaRDD<Vector> filtered = rddGoupedStatesPerMonth
                    .filter(x -> x._1().equals(finalMonth))
                    .flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Double, String>>>, Vector>) input13 -> {
                        List<Vector> result = new ArrayList<>();
                        for (Tuple2<Double, String> tuple : input13._2()) {
                            Vector a = Vectors.dense(tuple._1());
                            result.add(a);
                        }
                        return result.iterator();
                    });


            KMeansModel clusters = KMeans.train(filtered.rdd(), Config.NUM_CLUSTERS, Config.NUM_ITERATIONS);

            System.out.println("*****Training*****");
            int clusterNumber = 0;
            System.out.println("Clusters for month: " + month);
            for (Vector center : clusters.clusterCenters()) {
                System.out.println("Cluster center for Clsuter " + (clusterNumber++) + " : " + center);
            }
            double cost = clusters.computeCost(filtered.rdd());
            System.out.println("\nCost: " + cost);

            // Evaluate clustering by computing Within Set Sum of Squared Errors
            double WSSSE = clusters.computeCost(filtered.rdd());
            System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

            try {
                FileUtils.forceDelete(new File("KMeansModel"));
                System.out.println("\nDeleting old com.afjcjsbx.sabdcovid.model completed.");
            } catch (IOException ignored) {
            }

            // Save and load com.afjcjsbx.sabdcovid.model
            //clusters.save(sparkContext.sc(), "KMeansModel");
            //System.out.println("\rModel saved to KMeansModel/");
            //KMeansModel sameModel = KMeansModel.load(sparkContext.sc(), "KMeansModel");

            // prediction for test vectors
            System.out.println("\n*****Prediction for month: " + month + "*****");


            List<Iterable<Tuple2<Double, String>>> coefficientState = rddGoupedStatesPerMonth.filter(x -> x._1().equals(finalMonth)).
                    map(Tuple2::_2).collect();

            for (Iterable<Tuple2<Double, String>> cf : coefficientState) {
                for (Tuple2<Double, String> t : cf) {
                    listMlibResults.add(new Tuple2<>(month, new Tuple2<>(t._2(), clusters.predict(Vectors.dense(t._1())))));
                }
            }


        }

        long fSparkKmeans = System.currentTimeMillis();
        System.out.printf("Total time to compute Spark MLib K-Means: %s ms\n", (fSparkKmeans - iSparkKmeans));


        // Mappo i risultati in un Pair RDD
        JavaRDD<Tuple2<Integer, Tuple2<String, Integer>>> rddTemp = sparkContext.parallelize(listMlibResults);
        rddOutMlib = rddTemp.mapToPair(x-> new Tuple2<>(x._1(), new Tuple2<>(x._2()._1(), x._2()._2())));

        List<Tuple2<Integer, Tuple2<String, Integer>>> listNaiveResults = new ArrayList<>();

        System.out.println("*****Start Naive K-Means*****");
        long iNaiveKmeans = System.currentTimeMillis();

        /**
         * Start Naive KMeans
         */
        for (int month = 0; month < rddGoupedStatesPerMonth.keys().collect().size(); month++) {

            int finalMonth = month;
            JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> rddMontlyTrends = rddGoupedStatesPerMonth.filter(x -> x._1().equals(finalMonth));

            NaiveKMeans naiveKMeans = new NaiveKMeans(rddMontlyTrends.values(), Config.NUM_CLUSTERS, Config.NUM_ITERATIONS);
            System.out.println("\n*****Prediction for month: " + month + "*****");

            System.out.println("*****Training*****");
            int clusterNumber = 0;
            System.out.println("Clusters for month: " + month);
            for (Double center : naiveKMeans.getCentroids()) {
                System.out.println("Cluster center for Cluster " + (clusterNumber++) + " : " + center);
            }


            for (Tuple2<Integer, Tuple2<Double, String>> tuple : naiveKMeans.getClusters().collect()) {
                listNaiveResults.add(new Tuple2<>(month, new Tuple2<>(tuple._2()._2(), tuple._1())));
            }

        }


        long fNaiveKmeans = System.currentTimeMillis();
        System.out.printf("Total time to compute Naive K-Means: %s ms\n", (fNaiveKmeans - iNaiveKmeans));

        JavaRDD<Tuple2<Integer, Tuple2<String, Integer>>> rddTemp1 = sparkContext.parallelize(listNaiveResults);
        rddOutNaive = rddTemp1.mapToPair(x-> new Tuple2<>(x._1(), new Tuple2<>(x._2()._1(), x._2()._2())));

        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete Query 3: %s ms\n", (finalTime - initialTime));

    }

    /**
     * Gestione della persistenza dei risultati del processamento
     */
    @Override
    public void store() {
        // Salvo i risultati sull'HDFS
        rddOutMlib.saveAsTextFile(Config.PATH_RESULT_QUERY_3_MLIB);
        rddOutNaive.saveAsTextFile(Config.PATH_RESULT_QUERY_3_NAIVE);

        try {
            // Connessione a redis
            RedisConnection jedis = new RedisConnection(Config.DEFAULT_REDIS_HOSTNAME);
            // Seleziono il database numero 3 per inserire i risultati relativi al KMeans di MLib
            jedis.conn().select(3);


            for (Tuple2<Integer, Tuple2<String, Integer>> record : rddOutMlib.collect()) {
                jedis.conn().set(
                        "MLib month: " + record._1() + " State: " + record._2()._1(),
                        String.format(
                                "MLib ** Month: %s, State: %s ** Cluster: %d",
                                record._1(),
                                record._2()._1(),
                                record._2()._2()
                        ));
            }


            // Seleziono il database numero 4 per inserire i risultati relativi al KMeans Naive
            jedis.conn().select(4);

            for (Tuple2<Integer, Tuple2<String, Integer>> record : rddOutNaive.collect()) {
                jedis.conn().set(
                        "Naive month: " + record._1() + " State: " + record._2()._1(),
                        String.format(
                                "Naive ** Month: %s, State: %s ** Cluster: %d",
                                record._1(),
                                record._2()._1(),
                                record._2()._2()
                        ));
            }

        } catch (JedisConnectionException e) {
            e.printStackTrace();
        }


    }


}
