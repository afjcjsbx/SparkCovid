import data.Covid2Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Covid1Data;
import utils.DataParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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



        for (Covid2Data mm : countries.collect()){
            System.out.println(mm.toString());
        }



        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));


    }
}
