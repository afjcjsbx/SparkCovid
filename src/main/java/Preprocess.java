
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import utils.*;

import java.io.*;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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


        String initialDate = weeklyDate.first().getData();
        String finalDate = weeklyDate.take((int) weeklyDate.count()).get(0).getData();

        System.out.println("initialDate: " + initialDate);
        System.out.println("finalDate: " + finalDate);

        // Extract words within a tweet
        JavaRDD<Tuple2<String, Integer>> datas =
                weeklyDate.map(line -> new Tuple2<>(line.getData(), line.getDimessi_guariti()));


        JavaRDD<String> d = sc.textFile("src/main/resources/dataset1.csv");

        String header1 = d.first();
        JavaRDD<String> rowRdd = d.filter(line -> !line.equals(header1) );



        JavaPairRDD<String, Integer> dataPairRdd = rowRdd.mapToPair((String s) ->{
            String[] arr = s.split(",");
            String[] timestamp = arr[0].split("T");
            return new Tuple2<String, Integer>(timestamp[0], Integer.parseInt(arr[8]));
        });




        JavaPairRDD<String, Integer> groupedRdd = dataPairRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> sensorValueDay) throws Exception {

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                Date date = df.parse(sensorValueDay._1());

                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                int week = cal.get(Calendar.WEEK_OF_YEAR);

                return new Tuple2<String, Integer> (Integer.toString(week), sensorValueDay._2());
            }
        }).reduceByKey((x, y) -> (x + y) / 7);

        // collect RDD for printing
        for(Tuple2<String, Integer> line:groupedRdd.collect()){
            System.out.println("* "+line);
        }
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