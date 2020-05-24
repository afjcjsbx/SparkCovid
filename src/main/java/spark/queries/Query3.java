package spark.queries;

import lombok.Getter;
import model.Covid2Data;
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
import scala.Tuple2;
import spark.helpers.Common;
import spark.helpers.NaiveKMeans;
import utils.DataParser;
import utils.LinearRegression;
import utils.RegionParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;


public class Query3 implements IQuery {

    // KMeans parameters
    private static int NUM_CLUSTERS = 4;
    private static int NUM_ITERATIONS = 20;


    private final JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid2Data> rddIn;
    @Getter
    private JavaPairRDD<Tuple2<String, Integer>, Integer> rddOut;
    @Getter
    private JavaPairRDD<String, String> rddPairCountryContinent;

    private static String datasetPath = "src/main/resources/dataset2.csv";
    private static String regionPath = "src/main/resources/country_continent.csv";


    public Query3(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * Caricamento dei dati dal data store HDFS
     */
    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(datasetPath);
        String header = input.first();


        //get ther other lines of csv file
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        // Extract and parse tweet
        rddIn = rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header)).cache();


        //Load RDD regions mapping
        JavaRDD<String> rddRegions = sparkContext.textFile(regionPath);
        String headerRegion = rddRegions.first();
        rddRegions = rddRegions.filter(x -> !x.equals(headerRegion));
        rddPairCountryContinent = rddRegions
                .mapToPair(x -> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountry(), RegionParser.parseCSVRegion(x).getContinent()));

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files: %s ms\n", (fParseFile - iParseFile));


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


        // Creo un RDD composto da una String che rappresenta il nome dello stato e i relativi dati di quello stato
        JavaPairRDD<String, Covid2Data> rddStateData = rddIn.mapToPair(x -> new Tuple2<>(x.getState(), x));


        // Creo un RDD composto da una String che rappresenta il nome del continente e una lista di interi in
        // cui ci sono i casi totali relativi giorno per giorno
        JavaPairRDD<String, ArrayList<Integer>> rdd_region_final = rddIn.mapToPair(x -> new Tuple2<>(x.getState(), x.getCases()));


        // Creo un RDD composto da una Tupla2<String, String> che avrà il nome del continente come
        // primo campo e il numero del mese come secondo campo, Il valore Integer rappresenta
        // il numero dei casi relativi per ogni giorno di quel mese per ogni stato quindi conterrà
        // n tuple dove n sono tutti gli stati
        JavaPairRDD<String, ArrayList<Integer>> rddContinentDayByDay = rdd_region_final
                .reduceByKey((Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>) (arr1, arr2) -> {
                    ArrayList<Integer> sum = new ArrayList<>();
                    for (int z = 0; z < arr1.size(); z++)
                        sum.add(arr1.get(z) + arr2.get(z));
                    return sum;
                });


        // Da implementare nel load
        JavaRDD<String> input = sparkContext.textFile(datasetPath);
        String header = input.first();
        String[] cols = header.split(",");
        ArrayList<String> dates = new ArrayList<>(Arrays.asList(cols).subList(4, cols.length));
        ArrayList<Integer> months = new ArrayList<>();
        for (String date : dates) {
            months.add(Common.getMonthFromDate(date));
        }


        // Creo un RDD composto da una Tupla2<String, String> che avrà il nome del continente come
        // primo campo e il numero della settimana come secondo campo, Il valore Integer rappresenta
        // il numero dei casi relativi per un giorno di quella settimana della somma di tutti i casi
        // per ogni stato
        JavaPairRDD<Tuple2<String, Integer>, Integer> rddComplete = rddContinentDayByDay
                .flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>, Tuple2<String, Integer>, Integer>) arrayListTuple2 -> {
                    ArrayList<Tuple2<Tuple2<String, Integer>, Integer>> result_flat = new ArrayList<>();

                    for (int i = 1; i < months.size(); i++) {
                        Tuple2<Tuple2<String, Integer>, Integer> temp = new Tuple2<>(
                                new Tuple2<>(arrayListTuple2._1(), months.get(i)), arrayListTuple2._2().get(i));
                        result_flat.add(temp);
                    }

                    return result_flat.iterator();
                });


        // Raggruppo per chiave : <Stato ,Mese> ottenend un rdd <Stato ,Mese>, Iterable
        // dei nuovi casi del mese giorno per giorno
        JavaPairRDD<Tuple2<String, Integer>, Iterable<Integer>> rddTotalCasesInStateByMonth = rddComplete.groupByKey();


        // Colcoliamo il trend mese per mese per ogni stato ottenendo cos' un RDD che ha come
        // chiave il mese e come campo una tupla2 contenente come primo valore il Trend e come
        // secondo valore il nome dello stato
        JavaPairRDD<Integer, Tuple2<Double, String>> grouped = rddTotalCasesInStateByMonth.mapToPair((PairFunction<Tuple2<Tuple2<String, Integer>, Iterable<Integer>>, Integer, Tuple2<Double, String>>) input1 -> {

            String state_name = input1._1()._1();
            Integer month = input1._1()._2();
            ArrayList<Integer> casesPerMonth = new ArrayList<>();

            for (Integer value : input1._2()) {
                casesPerMonth.add(value);
            }

            double res = new LinearRegression(casesPerMonth).getCoefficient();
            return new Tuple2<>(month, new Tuple2<>(res, state_name));
        });




/*
        Una volta ottenuti i trend per ogni mese, raggruppiamo per chiave ottenendo un pair rdd composto da:
        <Mese>,<Iterable<Trend,Nome dello stato>>
         */
        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> resultGrouped = grouped.groupByKey();


        //Definiamo un arrayList contente le tuple del tipo <Numero Mese, Lista di Tuple< Trend, Nome stato >> corrispettivi al mese
        List<Tuple2<Integer, List<Tuple2<Double, String>>>> listTopStatesPerMonth = new ArrayList<>();


        for (int i = 0; i < resultGrouped.countByKey().size(); i++) {

            int fI = i;
            JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> rddMonthsStates = resultGrouped.filter(x -> x._1().equals(fI));
            JavaPairRDD<Double, String> rddMonthsCoefficient = rddMonthsStates.
                    flatMapToPair((PairFlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Double, String>>>, Double, String>) input12 -> {

                        ArrayList<Tuple2<Double, String>> result = new ArrayList<>();
                        for (Tuple2<Double, String> tuple : input12._2()) {
                            result.add(tuple);
                        }
                        return result.iterator();
                    });

            List<Tuple2<Double, String>> top = rddMonthsCoefficient.sortByKey(false).take(50);

            listTopStatesPerMonth.add(new Tuple2<>(fI, top));
        }


        JavaRDD<Tuple2<Integer, List<Tuple2<Double, String>>>> input2 = sparkContext.parallelize(listTopStatesPerMonth);

              /*
        prendiamo il nostro RDD creato precedentemente e lo trasformiamo in pair rdd cosi ottenuto:
        <mese><top 50 stati per mese>
         */
        JavaPairRDD<Integer, Tuple2<Double, String>> pairRddTopStatesPerMont = input2.
                flatMapToPair((PairFlatMapFunction<Tuple2<Integer, List<Tuple2<Double, String>>>, Integer, Tuple2<Double, String>>) row -> {
                    ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();
                    for (Tuple2<Double, String> tuple : row._2()) {
                        res.add(new Tuple2<>(row._1(), tuple));
                    }
                    return res.iterator();
                });


        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> temp4 = pairRddTopStatesPerMont.groupByKey();


        List<Tuple2<Integer, Tuple2<String, Integer>>> listMlibResults = new ArrayList<>();

        long iSparkKmeans = System.currentTimeMillis();


        for (int month = 0; month < temp4.keys().collect().size(); month++) {
            int finalMonth = month;
            JavaRDD<Vector> filtered = temp4
                    .filter(x -> x._1().equals(finalMonth))
                    .flatMap((FlatMapFunction<Tuple2<Integer, Iterable<Tuple2<Double, String>>>, Vector>) input13 -> {
                        ArrayList<Vector> result = new ArrayList<>();
                        for (Tuple2<Double, String> tupla : input13._2()) {
                            Vector a = Vectors.dense(tupla._1());
                            result.add(a);
                        }
                        return result.iterator();
                    });


            KMeansModel clusters = KMeans.train(filtered.rdd(), NUM_CLUSTERS, NUM_ITERATIONS);

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
                System.out.println("\nDeleting old model completed.");
            } catch (IOException ignored) {
            }

            // Save and load model
            //clusters.save(sparkContext.sc(), "KMeansModel");
            System.out.println("\rModel saved to KMeansModel/");
            //KMeansModel sameModel = KMeansModel.load(sparkContext.sc(), "KMeansModel");

            // prediction for test vectors
            System.out.println("\n*****Prediction for month: " + month + "*****");


            List<Iterable<Tuple2<Double, String>>> coefficientState = temp4.filter(x -> x._1().equals(finalMonth)).
                    map(Tuple2::_2).collect();

            for (Iterable<Tuple2<Double, String>> cf : coefficientState) {
                for (Tuple2<Double, String> t : cf) {
                    listMlibResults.add(new Tuple2<>(month, new Tuple2<>(t._2(), clusters.predict(Vectors.dense(t._1())))));
                }
            }


        }

        long fSparkKmeans = System.currentTimeMillis();
        System.out.printf("Total time to compute Spark MLib K-Means: %s ms\n", (fSparkKmeans - iSparkKmeans));

        for (Tuple2<Integer, Tuple2<String, Integer>> j : listMlibResults) {
            System.out.println(j);
        }


        List<Tuple2<Integer, Tuple2<String, Integer>>> listNaiveResults = new ArrayList<>();

        System.out.println("*****Start Naive K-Means*****");
        long iNaiveKmeans = System.currentTimeMillis();

        // Naive K-Means
        for (int month = 0; month < temp4.keys().collect().size(); month++) {

            int finalMonth = month;
            JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> rddMontlyTrends = temp4.filter(x -> x._1().equals(finalMonth));

            NaiveKMeans naiveKMeans = new NaiveKMeans(rddMontlyTrends.values(), NUM_CLUSTERS, NUM_ITERATIONS);
            System.out.println("\n*****Prediction for month: " + month + "*****");

            System.out.println("*****Training*****");
            int clusterNumber = 0;
            System.out.println("Clusters for month: " + month);
            for (Double center : naiveKMeans.getCentroids()) {
                System.out.println("Cluster center for Cluster " + (clusterNumber++) + " : " + center);
            }

            // prediction for test vectors

            //naiveKMeans.plotCluster();


            for (Tuple2<Integer, Tuple2<Double, String>> tuple : naiveKMeans.getClusters().collect()) {
                listNaiveResults.add(new Tuple2<>(month, new Tuple2<>(tuple._2()._2(), tuple._1())));
            }

        }


        for (Tuple2<Integer, Tuple2<String, Integer>> j : listNaiveResults) {
            System.out.println(j);
        }


        long fNaiveKmeans = System.currentTimeMillis();
        System.out.printf("Total time to compute Naive K-Means: %s ms\n", (fNaiveKmeans - iNaiveKmeans));






/*
        // <Month, (State, Cluster)>
        for (Tuple2<Integer, Tuple2<String, Integer>> j : to_file) {
            System.out.println(j);
        }









        NaiveKMeans naiveKMeans = new NaiveKMeans(temp4.values(), NUM_CLUSTERS, NUM_ITERATIONS);
        naiveKMeans.start();
        //naiveKMeans.plotCluster();
        naiveKMeans.plotCentroids();



 */

        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", (finalTime - initialTime));


    }

    /**
     * Gestione della persistenza dei risultati del processamento
     */
    @Override
    public void store() {

    }


}
