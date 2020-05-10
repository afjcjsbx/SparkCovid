
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import utils.CSVReaderInJava;
import utils.Covid1Data;
import utils.FileUtils;

import java.util.ArrayList;
import java.util.List;

public class Preprocess {

    public static void main(String[] args) {

        String pathFileWeatherDescription = "hdfs://0.0.0.0.:54310/dpc-covid19-ita-andamento-nazionale.csv";

        //-------------------------------------------------------

        //local mode
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");


        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        long start = System.currentTimeMillis();


        //read file
        JavaRDD<String> file= sc.textFile(pathFileWeatherDescription);
        String header = file.first();
        String[] firstLine = header.split(",",-1);


        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        //JavaRDD<ArrayList<Covid1Data>>  listOflistOfcities = otherLines.map(line -> CSVReaderInJava.createData(line));
        long fParseFile = System.currentTimeMillis();



        long stop = System.currentTimeMillis();
        System.out.println("ELAPSED TIME: ----> " + (stop-start)/1000);

        sc.stop();
    }
}