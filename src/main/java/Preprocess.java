
import helpers.Common;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import utils.*;

import java.io.*;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Preprocess {

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

        JavaRDD<String> input = sc.textFile("src/main/resources/dataset1.csv");
        String header = input.first();
        String[] firstLine = header.split(",", -1);


        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = input.filter(row -> !row.equals(header));
        JavaRDD<Covid1Data> weeklyDate = otherLines
                .map(line -> DataParser.parseCSV(line));
        long fParseFile = System.currentTimeMillis();



        JavaRDD<String> d = sc.textFile("src/main/resources/dataset1.csv");

        String header1 = d.first();
        JavaRDD<String> rowRdd = d.filter(line -> !line.equals(header1));


        JavaPairRDD<String, Tuple2<Integer, Integer>> clickstreamRDD = rowRdd.mapToPair((String s) -> {

            String[] arr = s.split(",");
            String[] timestamp = arr[0].split("T");

            int guariti = Integer.parseInt(arr[9]);

            int initial_week = 9;
            String week = Integer.toString(Common.getWeekFrom(timestamp[0]) - initial_week);
            System.out.println("Tuple: " + week + " " + guariti);


            return new Tuple2<String, Tuple2<Integer, Integer>>(week, new Tuple2(guariti, guariti));
        }).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) throws Exception {

                return new Tuple2(Math.max(tuple1._1(), tuple1._1()), Math.min(tuple2._1(), tuple2._2()));
            }
        });


        class GetLength implements Function<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Integer>> {
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> s) {
                return new Tuple2<String, Integer>(s._1(), (s._2._2() - s._2()._1()) / 7);
            }
        }


        JavaRDD<Tuple2<String, Integer>> values = clickstreamRDD.map(new GetLength());


        for (Tuple2<String, Integer> line : values.collect()) {
            System.out.println("* " + line);
        }



        /*
        JavaPairRDD<String, Tuple2<Integer, Integer>> dataPairRdd = rowRdd.mapToPair((String s) ->{
            String[] arr = s.split(",");
            String[] timestamp = arr[0].split("T");
            return new Tuple2<String, Tuple2<Integer, Integer>>(timestamp[0], new Tuple2<>(Integer.parseInt(arr[9]), Integer.parseInt(arr[12])));
        });




        JavaPairRDD<String, Tuple2<Integer, Integer>> groupedRdd = dataPairRdd.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Tuple2<Integer, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                Date date = df.parse(stringTuple2Tuple2._1());

                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                int week = cal.get(Calendar.WEEK_OF_YEAR);
                System.out.println("Tuple: " + week + " " + stringTuple2Tuple2._2()._1() + " " + stringTuple2Tuple2._2()._2());

                return new Tuple2<String, Tuple2<Integer, Integer>>(Integer.toString(week), new Tuple2(stringTuple2Tuple2._2()._1(), stringTuple2Tuple2._2()._2()));
            }

        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> reducedRdd = groupedRdd.reduceByKey(new Function2<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>() {

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> t1, Tuple2<String, Tuple2<Integer, Integer>> t2) throws Exception {
                int max_g = Math.max(t1._2()._1(), t1._2()._2());
                int min_g = Math.min(t1._2()._1(), t1._2()._2());

                return new Tuple2<Integer, Integer>(max_g, min_g);
            }
        });


         */


        // collect RDD for printing

        /*
        .mapToPair((String s, Integer i) ->{
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            Date date = df.parse(s);

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int week = cal.get(Calendar.WEEK_OF_YEAR);

            return new Tuple2<String, Integer>(Integer.toString(week), i);
        });
         */


        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));
    }

}