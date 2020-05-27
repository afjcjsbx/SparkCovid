package com.afjcjsbx.sabdcovid.spark.queries;

import lombok.Getter;
import com.afjcjsbx.sabdcovid.model.Config;
import com.afjcjsbx.sabdcovid.model.Covid2Data;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import scala.Tuple2;
import com.afjcjsbx.sabdcovid.spark.helpers.Common;
import com.afjcjsbx.sabdcovid.utils.DataParser;
import com.afjcjsbx.sabdcovid.utils.LinearRegression;
import com.afjcjsbx.sabdcovid.utils.RegionParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query2 implements IQuery {

    private final JavaSparkContext sparkContext;

    @Getter
    private JavaRDD<Covid2Data> rddIn;
    @Getter
    private JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<Tuple2<Integer, Integer>, Double>, Double>> rddOut;
    @Getter
    private JavaPairRDD<String, String> rddPairCountryContinent;



    public Query2(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * Caricamento dei dati dal data store HDFS
     */
    @Override
    public void load() {

        long iParseFile = System.currentTimeMillis();

        JavaRDD<String> input = sparkContext.textFile(Config.PATH_DATASET_2);
        String header = input.first();



        //get ther other lines of csv file
        JavaRDD<String> rowRdd = input.filter(row -> !row.equals(header));
        // Extract and parse tweet
        rddIn = rowRdd.map(line -> DataParser.parseCSVcovid2data(line, header)).cache();



        //Load RDD regions mapping
        JavaRDD<String> rddRegions = sparkContext.textFile(Config.PATH_COUNTRY_CONTINENT);
        String headerRegion = rddRegions.first();
        rddRegions = rddRegions.filter(x->!x.equals(headerRegion));
        rddPairCountryContinent = rddRegions
                .mapToPair(x-> new Tuple2<>(RegionParser.parseCSVRegion(x).getCountry(), RegionParser.parseCSVRegion(x).getContinent()));

        long fParseFile = System.currentTimeMillis();
        System.out.printf("Total time to parse files in Query 2: %s ms\n", (fParseFile - iParseFile));


    }

    /**
     * Esecuzione della query 2.
     *
     * QUERY 2:
     *      Per ogni continente calcolare la media, deviazione standard, minimo, massimo del
     *      numero di casi confermati su base settimanale.
     *      Nota: Considerare solo i maggiori 100 stati colpiti, per calcolare gli stati
     *      maggiormente colpiti andiamo a calcolare il coefficiente di trenline utilizzando
     *      una regressione lineare.
     */
    @Override
    public void execute() {

        long initialTime = System.currentTimeMillis();

        // Calcolo il coefficiente di trendline per ogni stato, lo metto in un RDD con il nome
        // dello stato e lo ordino in modo decrescente dallo stato più colpito a quello meno colpito
        JavaPairRDD<Double, String> rddTrends = rddIn.mapToPair((PairFunction<Covid2Data, Double, String>) value -> {
            double slope = new LinearRegression(value.getCases()).getCoefficient();
            return new Tuple2<>(slope, value.getState());
        }).sortByKey(false);


        // Considero i 100 stati meggiormente colpiti
        List<Tuple2<Double, String>> pairTop = rddTrends.sortByKey(false).take(100);

        // Convero la lista precedentemente calcolata in un RDD
        JavaRDD<Tuple2<Double, String>> input2 = sparkContext.parallelize(pairTop);

        // Creo un RDD composto da una String che rappresenta il nome dello stato e i relativi dati di quello stato
        JavaPairRDD<String,Covid2Data> rddStateData = rddIn.mapToPair(x-> new Tuple2<>(x.getState(),x));

        // Creo un RDD composto da una String che rappresenta il nome dello stato e una Tupla2 che ha come
        // primo campo i dati relativi a quello stato e come secondo campo il continente a cui quello stato
        // fa parte
        JavaPairRDD<String, Tuple2<Covid2Data, String>> rddContinents = rddStateData.join(rddPairCountryContinent);


        // Creo un RDD composto da una String che rappresenta il nome del continente e una lista di interi in
        // cui ci sono i casi totali relativi giorno per giorno
        JavaPairRDD <String, ArrayList<Integer>> rdd_region_final = rddContinents.mapToPair(x-> new Tuple2<>(x._2()._2(), x._2()._1().getCases()));


        // Creo un RDD composto da una Tupla2<String, String> che avrà il nome del continente come
        // primo campo e il numero della settimana come secondo campo, Il valore Integer rappresenta
        // il numero dei casi relativi per un giorno di quella settimana per ogni stato quindi conterrà
        // n tuple dove n sono tutti gli stati
        JavaPairRDD<String, ArrayList<Integer>> rddContinentDayByDay = rdd_region_final
                .reduceByKey((Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>) (arr1, arr2) -> {
                    ArrayList<Integer> sum = new ArrayList<>();
                    for(int z = 0; z < arr1.size(); z++)
                        sum.add(arr1.get(z) + arr2.get(z));
                    return sum;
                });





        // Da implementare nel load
        JavaRDD<String> input = sparkContext.textFile(Config.PATH_DATASET_2);
        String header = input.first();
        String[] cols = header.split(",");
        ArrayList<String> dates = new ArrayList<>(Arrays.asList(cols).subList(4, cols.length));
        ArrayList<String> weeks = new ArrayList<>();
        for (String date : dates) {
            weeks.add(Common.getWeekFrom2(date).toString());
        }


        // Creo un RDD composto da una Tupla2<String, String> che avrà il nome del continente come
        // primo campo e il numero della settimana come secondo campo, Il valore Integer rappresenta
        // il numero dei casi relativi per un giorno di quella settimana della somma di tutti i casi
        // per ogni stato
        JavaPairRDD<Tuple2<String,String>,Integer> rddComplete = rddContinentDayByDay
                .flatMapToPair((PairFlatMapFunction<Tuple2<String, ArrayList<Integer>>, Tuple2<String, String>, Integer>) arrayListTuple2 -> {
                    ArrayList<Tuple2<Tuple2<String, String>,Integer>> result_flat = new ArrayList<>();

                    for(int i = 0;i < weeks.size(); i++){
                        Tuple2<Tuple2<String,String>,Integer> temp = new Tuple2<>(
                                new Tuple2<>(arrayListTuple2._1(), weeks.get(i)), arrayListTuple2._2().get(i));
                                result_flat.add(temp);
                    }

                    return result_flat.iterator();
                });





        // Accorpo l'RDD che continene una Tupla2(Continente, Settimana), CasiTotaliSettimana)
        JavaPairRDD <Tuple2<String, String>, Integer> continentTotalCasesPerWeek = rddComplete.reduceByKey(Integer::sum);


        // RDD che continene una (Tupla2(Continente, Settimana), MaxNumeroDiCasiInUnGiornoDiQuellaSettimana)
        JavaPairRDD<Tuple2<String, String>, Integer> maxContinentTotalCasesPerWeek = rddComplete.reduceByKey(Math::max);

        // RDD che continene una (Tupla2(Continente, Settimana), MinNumeroDiCasiInUnGiornoDiQuellaSettimana)
        JavaPairRDD<Tuple2<String, String>, Integer> minContinentTotalCasesPerWeek = rddComplete.reduceByKey(Math::min);



        JavaPairRDD<Tuple2<String, String>, Integer> unit = rddComplete.mapToPair(x-> new Tuple2<>(x._1(), 1));
        // RDD (Continent, Week) , 7)
        JavaPairRDD <Tuple2<String, String>, Integer> count = unit.reduceByKey(Integer::sum);

        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> joinres = count.join(continentTotalCasesPerWeek);

        JavaPairRDD <Tuple2<String,String>, Double> averageStateCasesPerWeek = joinres.mapToPair(x-> new Tuple2<>(x._1(), Double.parseDouble(String.valueOf(x._2()._2() / x._2()._1()))));


        // RDD che continene una (Tupla2(Continente, Settimana), DeviazioneStandardPerQuellaSettimana)
        JavaPairRDD<Tuple2<String,String>, Double> stdDevContinentPerWeek = averageStateCasesPerWeek.join(rddComplete)
                .mapToPair(x->new Tuple2<>(x._1(), Math.pow(x._2()._1() - x._2()._2(), 2)))
                .reduceByKey(Double::sum).join(count)
                .mapToPair(x->new Tuple2<>(x._1(), Math.sqrt(x._2()._1() / x._2()._2())));




        rddOut = maxContinentTotalCasesPerWeek.join(minContinentTotalCasesPerWeek).join(averageStateCasesPerWeek).join(stdDevContinentPerWeek);
        for(Tuple2<Tuple2<String, String>, Tuple2<Tuple2<Tuple2<Integer, Integer>, Double>, Double>> s : rddOut.collect()){
            System.out.println(s);
        }


        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete Query 2: %s ms\n", (finalTime - initialTime));


    }

    /**
     * Gestione della persistenza dei risultati del processamento
     */
    @Override
    public void store() {

        rddOut.saveAsTextFile(Config.PATH_RESULT_QUERY_2);

        this.rddOut.foreachPartition(partition -> partition.forEachRemaining(record -> {
            try{
                Jedis jedis = new Jedis("localhost");
                jedis.select(2);

                jedis.set(
                        "State :" + record._1()._1() + " Week: " +record._1()._2(),
                        String.format(
                                "** State: %s - Week: %s ** Max: %s, Min: %s, AVG: %s, STD: %s",
                                record._1()._1(),
                                record._1()._2(),
                                record._2()._1()._1()._1(),
                                record._2()._1()._1()._2(),
                                record._2()._1()._2(),
                                record._2()._2()
                        ));

            } catch (JedisConnectionException e){
                e.printStackTrace();
            }

        }));



    }



}
