import data.Covid2Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.ConvertData;
import utils.DataParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Calendar;

public class Query3 {

    String pathFileWeatherDescription = "hdfs://127.0.0.1:54310/dpc-covid19-ita-andamento-nazionale.csv";

    public static void main(String[] args) throws IOException, URISyntaxException {

        long initialTime = System.currentTimeMillis();

        //Configurazione di Spark
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        conf.set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        long iOperations = System.currentTimeMillis();

        //Apro il file
        JavaRDD<String> input = sc.textFile("src/main/resources/dataset2.csv");
        String header = input.first();
        String[] firstLine = header.split(",", -1);
        final Integer num_of_days = firstLine.length - 4; //I primi 4 sono parametri fissi

        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = input.filter(row -> !row.equals(header));
        JavaRDD<Covid2Data> weeklyDate = otherLines
                .map(line -> DataParser.parseCSVcovid2data(line, num_of_days, firstLine));
        long fParseFile = System.currentTimeMillis();

        String initialDate = firstLine[4];
        String finalDate = firstLine[firstLine.length];

        System.out.println("initialDate: " + initialDate);
        System.out.println("finalDate: " + finalDate);

        //Convert String to data format (m/gg/aa)
        Calendar data_inizio = ConvertData.convert(initialDate);
        Calendar data_fine = ConvertData.convert(finalDate);

        /*
        //Finestra scorrevole dei mesi
        Calendar newInit = data_inizio;

        //Vai alla fine del mese
        Calendar a = ConvertData.goInitNextMonth(data_inizio);
        Calendar newFinish = ConvertData.goToEndOfTheMonth(data_inizio);
        Calendar newFinish = ConvertData.addMonth(data_inizio);
        Calendar newFinish = ConvertData.nextMonth(data_inizio);

        //Calcolo la differenza dei giorni
        long noOfDaysBetween  = ChronoUnit.DAYS.between(newInit,newFinish);

        CALCOLO TRENDLINE ();

        //Aggiorno date di inizio e fine
        newInit = newFinish;
        newFinish = ConvertData.nextMonth(data_inizio);




                // Extract words within a tweet
        JavaRDD<Tuple2<String, Integer>> values =
                weeklyDate.map(line -> new Tuple2<>(((line.getState() != null) ? line.getStato() : line.getRegione()), line.calcolaTrend()));



         */

        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));
    }
}
