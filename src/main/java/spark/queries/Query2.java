package spark.queries;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import model.Covid2Data;
import spark.helpers.Common;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.queries.IQuery;
import utils.DataParser;
import utils.LinearRegression;
import utils.RegionParser;

import java.util.*;

public class Query2 implements IQuery {

    private JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid2Data> rddIn;
    @Getter
    private JavaPairRDD<Tuple2<String, Integer>, Integer> rddOut;
    @Getter
    private JavaPairRDD<String, String> rddPair_region;
    @Getter
    private ArrayList<String> weeks;

    private static String datasetPath = "src/main/resources/dataset2.csv";
    private static String regionPath = "src/main/resources/country_continent.csv";


    public Query2(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }


    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(datasetPath);
        String header = input.first();

        String[] cols = header.split(",");
        ArrayList<String> dates = new ArrayList<>(Arrays.asList(cols).subList(4, cols.length));
        weeks = new ArrayList<>();
        for (String date : dates) {
            weeks.add(Common.getWeekFrom2(date).toString());
        }

        //get ther other lines of csv file
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        // Extract and parse tweet
        rddIn = rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header)).cache();


        //Load RDD regions mapping
        JavaRDD<String> rddRegions = sparkContext.textFile(regionPath);
        String headerRegion = rddRegions.first();
        JavaRDD<String> rdd_regio_withoutFirst = rddRegions.filter(x->!x.equals(headerRegion));
        rddPair_region = rdd_regio_withoutFirst
                .mapToPair(x-> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountry(), RegionParser.parseCSVRegion(x).getContinent()));

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files: %s ms\n", (fParseFile - iParseFile));

    }

    @Override
    public void execute() {

        long initialTime = System.currentTimeMillis();


        //Find Trendline coefficient for each country/state
        JavaPairRDD<Double, String> rddTrends = rddIn.mapToPair(new PairFunction<Covid2Data, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Covid2Data value) {
                double slope = new LinearRegression(value.getCases()).getCoefficient();
                return new Tuple2<>(slope, value.getCountry());
            }
        }).sortByKey(false);



        List<Tuple2<Double, String>> pairTop = rddTrends.sortByKey(false).take(50);


        JavaPairRDD<String,Covid2Data> result1 = rddIn.mapToPair(x-> new Tuple2<>(x.getCountry(),x));
        /*
         * ??????
         */
        JavaPairRDD <String, Covid2Data> resultfilter = result1.filter(new Function<Tuple2<String, Covid2Data>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Covid2Data> stringStateTuple2) {
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

        JavaPairRDD <String, ArrayList<Integer>> rdd_region_final = rdd_continents.mapToPair(x-> new Tuple2<>(x._2()._2(), x._2()._1().getCases()));



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



        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime - initialTime));


    }

    @Override
    public void store() {

    }



}
