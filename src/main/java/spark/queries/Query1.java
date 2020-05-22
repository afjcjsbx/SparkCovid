package spark.queries;

import lombok.Getter;
import model.Config;
import model.Covid1Data;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import spark.helpers.Common;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import utils.*;

public class Query1 implements IQuery {

    private static final Logger log = LogManager.getLogger(Query1.class);

    private final JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid1Data> rddIn;
    @Getter
    private JavaPairRDD<Integer, Tuple2<Double, Double>> rddOut;

    private static String datasetPath = "src/main/resources/dataset1.csv";

    public Query1(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }


    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(datasetPath);
        String header = input.first();

        //get ther other lines of csv file
        JavaRDD<String> otherLines = input.filter(row -> !row.equals(header));
        rddIn = otherLines.map(line -> DataParser.parseCSV(line));

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files: %s ms\n", Long.toString(fParseFile - iParseFile));
    }

    /**
     * Esecuzione della query 1.
     *
     * QUERY 1:
     *      Per ogni settimana calcolare il numero medio di persone curate e il numero
     *      medio di tamponi effettuati.
     */
    @Override
    public void execute() {

        long initialTime = System.currentTimeMillis();

        // Creo un RDD composto da un Integer che mi rappresenta il numero della settimana e il
        // secondo campo mantiene i dati relativi a quella settimana
        JavaPairRDD<Integer, Covid1Data> pair = rddIn.mapToPair((PairFunction<Covid1Data, Integer, Covid1Data>) value -> {
            int initial_week = 9;
            int week = Common.getWeekFrom(value.getData()) - initial_week;

            return new Tuple2<>(week, value);
        }).cache();


        // Creo un RDD composto da un Integer che mi rappresenta il numero della settimana e un
        // Double che rappresenta il numero medio di persone guarite in quella settimana
        JavaPairRDD<Integer, Double> rddWeeklyAvgHealed = pair
                .aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x.getDimessi_guariti()), StatCounter::merge)
                .mapToPair(x -> {
                    Integer key = x._1();
                    Double mean = x._2().mean();
                    return new Tuple2<>(key, mean);
                });


        // Creo un RDD composto da un Integer che mi rappresenta il numero della settimana e un
        // Double che rappresenta il numero medio di tamponi effettuati in quella settimana
        JavaPairRDD<Integer, Double> rddWeeklyAvgSwabds = pair
                .aggregateByKey(new StatCounter(), (acc, x) -> acc.merge(x.getTamponi()), StatCounter::merge)
                .mapToPair(x -> {
                    Integer key = x._1();
                    Double mean = x._2().mean();
                    return new Tuple2<>(key, mean);
                });


        //Unisco i due RDDs
        rddOut = rddWeeklyAvgHealed.join(rddWeeklyAvgSwabds).sortByKey();


        for (Tuple2<Integer, Tuple2<Double, Double>> string : rddOut.collect()) {
            System.out.println(string._1() + " " + string._2()._1() + " " + string._2()._2());
        }


        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", (finalTime - initialTime));

    }

    @Override
    public void store() {

        this.rddOut.foreachPartition(partition -> partition.forEachRemaining(record -> {
            try{
                Jedis jedis = new Jedis("localhost");
                jedis.select(1);

                jedis.set(
                        record._1().toString(),
                        String.format(
                                "**Week: %d** AVG Healed: %s, AVG Swads: %s",
                                record._1(),
                                record._2()._1(),
                                record._2()._2()
                        ));

            } catch (JedisConnectionException e){
                e.printStackTrace();
            }



        }));


    }
}