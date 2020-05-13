
import helpers.Common;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import utils.*;

import java.io.*;
import java.net.URISyntaxException;

public class Query1 {

    private static String datasetPath = "src/main/resources/dataset1.csv";

    public static void main(String[] args) throws IOException, URISyntaxException {

        long initialTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        conf.set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> input = sc.textFile(datasetPath);
        String header = input.first();

        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = input.filter(row -> !row.equals(header));
        JavaRDD<Covid1Data> weeklyDate = otherLines
                .map(line -> DataParser.parseCSV(line));
        long fParseFile = System.currentTimeMillis();

        System.out.printf("Total time to parse file: %s ms\n", (fParseFile - iParseFile));

        JavaPairRDD<Integer, Covid1Data> pair = weeklyDate.mapToPair((PairFunction<Covid1Data, Integer, Covid1Data>) value -> {
            int initial_week = 9;
            int week = Common.getWeekFrom(value.getData()) - initial_week;

            return new Tuple2<>(week, value);
        }).cache();


        //AVG Healed
        JavaPairRDD<Integer, Double> rddWeeklyAvgHealed = pair
                .aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x.getDimessi_guariti()), StatCounter::merge)
                .mapToPair(x -> {
                    Integer key = x._1();
                    Double mean = x._2().mean();
                    return new Tuple2<>(key, mean);
        });


        //AVG Swabds
        JavaPairRDD<Integer, Double> rddWeeklyAvgSwabds = pair
                .aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x.getTamponi()), StatCounter::merge)
                .mapToPair(x -> {
                    Integer key = x._1();
                    Double mean = x._2().mean();
                    return new Tuple2<>(key, mean);
        });

        //join RDDs
        JavaPairRDD<Integer, Tuple2<Double, Double>> resultRDD = rddWeeklyAvgHealed.join(rddWeeklyAvgSwabds).sortByKey();


        for (Tuple2<Integer, Tuple2<Double, Double>> string : resultRDD.collect()) {
            System.out.println(string._1() + " " + string._2()._1() + " " + string._2()._2());
        }


        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));
    }

}