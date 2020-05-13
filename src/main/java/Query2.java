import data.Covid2Data;
import helpers.Common;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import utils.Covid1Data;
import utils.DataParser;
import utils.LinearRegression;
import utils.RegionParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class Query2 {

    private static String datasetPath = "src/main/resources/dataset2.csv";
    private static String regionPath = "src/main/resources/country_continent.csv";

    public static void main(String[] args) throws IOException, URISyntaxException {


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

        String[] cols = header.split(",");
        ArrayList<String> dates = new ArrayList<>(Arrays.asList(cols).subList(4, cols.length));
        ArrayList<String> weeks = new ArrayList<>();
        for (String date : dates) {
            weeks.add(Common.getWeekFrom2(date).toString());
        }


        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        // Extract and parse tweet
        JavaRDD<Covid2Data> data =
                rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header));


        //Load RDD regions mapping
        JavaRDD<String> rddRegions = sc.textFile(regionPath);
        String headerRegion = rddRegions.first();
        JavaRDD<String> rdd_regio_withoutFirst = rddRegions.filter(x->!x.equals(headerRegion));
        JavaPairRDD<String, String> rddPair_region = rdd_regio_withoutFirst
                .mapToPair(x-> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountry(), RegionParser.parseCSVRegion(x).getContinent()));

        long fParseFile = System.currentTimeMillis();

        // For each tweet t, we extract all the hashtags and create a pair (hashtag,user)
        //JavaPairRDD<String, Tuple2<String, Integer>> pairs = countries.flatMapToPair(new DaysToCountryExtractor());

/*
        JavaPairRDD<String, Tuple2<Integer, Integer>> pair = pairs.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<Integer, Integer>>() {

            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<String, Integer>> stringIntegerTuple2) throws Exception {
                int initial_week = 4;
                int week = Common.getWeekFrom2(stringIntegerTuple2._2()._1()) - initial_week;
                return new Tuple2<String, Tuple2<Integer, Integer>>(stringIntegerTuple2._1(), new Tuple2<Integer, Integer>(week, stringIntegerTuple2._2()._2()));
            }

        });

 */


        //Find Trendline coefficient for each country/state
        JavaPairRDD<Double, String> rddTrends = data.mapToPair(new PairFunction<Covid2Data, Double, String>() {
                    @Override
                    public Tuple2<Double, String> call(Covid2Data value) throws Exception {
                        double slope = new LinearRegression(value.getCases()).getCoefficient();
                        return new Tuple2<>(slope, value.getCountry());
                    }
                }).sortByKey(false);

        /*
        for (Tuple2<Double, String> string : rddTrends.collect()) {
            System.out.println(string._1() + " " + string._2());
        }

         */


        List<Tuple2<Double, String>> pairTop = rddTrends.sortByKey(false).take(50);


        JavaPairRDD<String,Covid2Data> result1 = data.mapToPair(x-> new Tuple2<>(x.getCountry(),x));
        /**
         * ??????
         */
        JavaPairRDD <String, Covid2Data> resultfilter = result1.filter(new Function<Tuple2<String, Covid2Data>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Covid2Data> stringStateTuple2) throws Exception {
                for(Tuple2<Double,String> i: pairTop ) {
                    if (stringStateTuple2._1().equals(i._2())){
                        return true;
                    }
                }
                return false;
            }
        });



        JavaPairRDD<String, Covid2Data> pairRDD_state_country = result1.mapToPair(x->new Tuple2<>(x._2().getCountry(),x._2()));

        JavaPairRDD<String, Tuple2<Covid2Data, String>> rdd_continents = pairRDD_state_country.join(rddPair_region);

        JavaPairRDD <String, ArrayList<Integer>> rdd_region_final = rdd_continents.mapToPair(x-> new Tuple2<String, ArrayList<Integer>>(x._2()._2(), x._2()._1().getCases()));



        JavaPairRDD<String,ArrayList<Integer>> pairRDD_sum = rdd_region_final.
                reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
                    @Override
                    public ArrayList<Integer> call(ArrayList<Integer> integers, ArrayList<Integer> integers2) {
                        ArrayList<Integer> sum_result=new ArrayList<>();
                        for(int i=0;i<integers.size();i++)
                            sum_result.add(integers.get(i)+integers2.get(i));
                        return sum_result;
                    }
                });


        for (String j : weeks){
            System.out.println(j);
        }


        // contiene <continente,week>,somma valore per quel giorno per continente
        JavaPairRDD<Tuple2<String,String>,Integer> pair_flat = pairRDD_sum.
                flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>, Tuple2<String, String>, Integer>() {
                    @Override
                    public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(Tuple2<String, ArrayList<Integer>> stringArrayListTuple2) {
                        ArrayList<Tuple2<Tuple2<String, String>,Integer>> result_flat = new ArrayList<>();

                        for(int i = 0; i < weeks.size(); i++){
                            Tuple2<Tuple2<String,String>,Integer> temp = new Tuple2<>(
                                    new Tuple2<>(stringArrayListTuple2._1(), weeks.get(i)), stringArrayListTuple2._2().get(i));
                            result_flat.add(temp);
                        }
                        return result_flat.iterator();
                    }
                });


        for (Tuple2<Tuple2<String, String>, Integer> j : pair_flat.collect()){
            System.out.println(j);
        }



        long fOperations = System.currentTimeMillis();
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));


    }


    /*



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

     */




    private static class WeeklyDataExtractor implements Function2<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>> {

        @Override
        public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple2, Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple22) throws Exception {
            return new Tuple2<String, Tuple2<String, Integer>>(stringTuple2Tuple2._1(), new Tuple2<String, Integer>(stringTuple2Tuple2._2()._1(), stringTuple2Tuple22._2()._2() - stringTuple2Tuple2._2()._2())) ;
        }
    }

}
