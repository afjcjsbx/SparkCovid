import data.Covid2Data;
import data.Covid2DataInner;
import data.Stats;
import helpers.Common;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import utils.Covid1Data;
import utils.DataParser;
import utils.LinearRegression;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    String pathFileWeatherDescription = "hdfs://127.0.0.1:54310/dpc-covid19-ita-andamento-nazionale.csv";


    public static void main(String[] args) throws IOException, URISyntaxException {

        /*
                if (args.length != 3) {
            System.out.println("\nERROR: Insert arguments in this order: " +
                    "1. 'file city-attributes, 2. file temperatures 3. redis ip'");
        }
        */

        long initialTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        conf.set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        long iOperations = System.currentTimeMillis();

        JavaRDD<String> input = sc.textFile("src/main/resources/dataset2.csv");
        String header = input.first();
        String[] firstLine = header.split(",", -1);


        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        long fParseFile = System.currentTimeMillis();


        // Extract and parse tweet
        JavaRDD<Covid2Data> countries =
                rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header));

        // For each tweet t, we extract all the hashtags and create a pair (hashtag,user)
        JavaPairRDD<String, Tuple2<String, Integer>> pairs = countries.flatMapToPair(new DaysToCountryExtractor());


        JavaPairRDD<String, Tuple2<Integer, Integer>> pair = pairs.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<Integer, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<String, Integer>> stringIntegerTuple2) throws Exception {
                int initial_week = 4;
                int week = Common.getWeekFrom2(stringIntegerTuple2._2()._1()) - initial_week;
                return new Tuple2<String, Tuple2<Integer, Integer>>(stringIntegerTuple2._1(), new Tuple2<Integer, Integer>(week, stringIntegerTuple2._2()._2()));
            }

        });



        for (Tuple2<String, Tuple2<Integer, Integer>> hh : pair.collect()) {
            System.out.println(hh.toString());
        }


        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));


    }



    private static class DaysToCountryExtractor implements PairFlatMapFunction<Covid2Data, String, Tuple2<String, Integer>> {

        @Override
        public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Covid2Data covid2Data) throws Exception {

            List<Tuple2<String, Tuple2<String, Integer>>> states_tuple = new ArrayList<>();

            for (Covid2DataInner covid2DataInner : covid2Data.getDays()) {
                Tuple2<String, Integer> subTuple = new Tuple2<String, Integer>(covid2DataInner.getDay(), covid2DataInner.getConfirmed_cases());
                Tuple2<String, Tuple2<String, Integer>> tuple2 = new Tuple2<String, Tuple2<String, Integer>>(covid2Data.getCountry(), subTuple);
                states_tuple.add(tuple2);
            }

            return states_tuple.iterator();
        }

    }




    private static class WeeklyDataExtractor implements Function2<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>> {

        @Override
        public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple2, Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple22) throws Exception {
            return new Tuple2<String, Tuple2<String, Integer>>(stringTuple2Tuple2._1(), new Tuple2<String, Integer>(stringTuple2Tuple2._2()._1(), stringTuple2Tuple22._2()._2() - stringTuple2Tuple2._2()._2())) ;
        }
    }

}
