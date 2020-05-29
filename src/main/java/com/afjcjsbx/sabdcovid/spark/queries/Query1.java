package com.afjcjsbx.sabdcovid.spark.queries;

import com.afjcjsbx.sabdcovid.model.RedisConnection;
import lombok.Getter;
import com.afjcjsbx.sabdcovid.model.Config;
import com.afjcjsbx.sabdcovid.model.Covid1Data;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import scala.Tuple2;
import com.afjcjsbx.sabdcovid.spark.helpers.Common;
import com.afjcjsbx.sabdcovid.utils.DataParser;

public class Query1 implements IQuery {

    private final JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid1Data> rddIn;
    @Getter
    private JavaPairRDD<Integer, Tuple2<Double, Double>> rddOut;


    public Query1(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }


    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(Config.PATH_DATASET_1);
        String header = input.first();

        //get ther other lines of csv file
        JavaRDD<String> otherLines = input.filter(row -> !row.equals(header));
        rddIn = otherLines.map(DataParser::parseCSV);

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files in Query 1: %s ms\n", (fParseFile - iParseFile));
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
            int week = Common.getWeekFrom(value.getData()) - Config.WEEK_OFFSET;

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



        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete Query 1: %s ms\n", (finalTime - initialTime));

    }

    /**
     * Gestione della persistenza dei risultati del processamento
     */
    @Override
    public void store() {
        // Salvo i risultati sull'HDFS
        rddOut.saveAsTextFile(Config.PATH_RESULT_QUERY_1);

        this.rddOut.foreachPartition(partition -> partition.forEachRemaining(record -> {
            try{
                // Connessione a redis
                RedisConnection jedis = new RedisConnection(Config.DEFAULT_REDIS_HOSTNAME);
                // Seleziono il database numero 2 per inserire i risultati
                jedis.conn().select(1);

                jedis.conn().set(
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