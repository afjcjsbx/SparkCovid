package spark.queries;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import spark.helpers.Common;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import spark.queries.IQuery;
import utils.*;

public class Query1 implements IQuery {

    private JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid1Data> rddIn;
    @Getter
    private JavaPairRDD<Tuple2<String, Integer>, Integer> rddOut;

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

    @Override
    public void execute() {

        long initialTime = System.currentTimeMillis();

        JavaPairRDD<Integer, Covid1Data> pair = rddIn.mapToPair((PairFunction<Covid1Data, Integer, Covid1Data>) value -> {
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


        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));

    }

    @Override
    public void store() {

    }
}