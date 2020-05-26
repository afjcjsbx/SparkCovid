package com.afjcjsbx.sabdcovid.spark.queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import com.afjcjsbx.sabdcovid.model.Covid1Data;
import com.afjcjsbx.sabdcovid.utils.DataParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class Query1SparksSQL {

    private static String pathToFile = "src/main/resources/dataset1.csv";

    public static void main(String[] args) throws IOException, URISyntaxException {


        long initialTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        conf.set("com.afjcjsbx.sabdcovid.spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //The entry point to programming Spark with the Dataset and DataFrame API.
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL query1").master("local")
                //.config("com.afjcjsbx.sabdcovid.spark.some.config.option", "some-value")
                .getOrCreate();



        JavaRDD<String> linesFile = sc.textFile("src/main/resources/dataset1.csv");

        String header1 = linesFile.first();
        JavaRDD<String> rowRdd = linesFile.filter(line -> !line.equals(header1));


        JavaRDD<Covid1Data> outlets = rowRdd.map(line -> DataParser.parseCSV(line));

        JavaRDD<Tuple3<String, Integer, Integer>> result = outlets.map(x -> new Tuple3<String, Integer, Integer>
                (x.getData(), x.getDimessi_guariti(), x.getTamponi()));

        Dataset<Row> df = createSchemaFromPreprocessedData(spark, result);

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("query1");

        Dataset<Row> results = spark.sql("SELECT date, cared, swabds FROM query1");
        //results.createOrReplaceTempView("temp");
        //Dataset<Row> sqlDF = com.afjcjsbx.sabdcovid.spark.sql("SELECT DISTINCT house_id FROM temp WHERE sum >= 350 ");


        results.show();
        spark.close();
    }


    private static Dataset<Row> createSchemaFromPreprocessedData(SparkSession spark, JavaRDD<Tuple3<String, Integer, Integer>> values){

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("date",      DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cared",     DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("swabds",         DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(new Function<Tuple3<String, Integer, Integer>, Row>() {
            @Override
            public Row call(Tuple3<String, Integer, Integer> val) throws Exception {
                return RowFactory.create(val._1(), val._2(), val._3());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        return df;

    }

}
