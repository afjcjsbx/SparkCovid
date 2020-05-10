
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import scala.Tuple3;
import utils.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;

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
        JavaRDD<Tuple3<String, Integer, Integer>> datas =
                weeklyDate.map(line -> new Tuple3<>(line.getData(), line.getDimessi_guariti(), line.getTamponi()));


        // collect RDD for printing
        for(Tuple3<String, Integer, Integer> line:datas.collect()){
            System.out.println("* "+line);
        }


        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));
    }

}