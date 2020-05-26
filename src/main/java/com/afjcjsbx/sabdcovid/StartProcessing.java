package com.afjcjsbx.sabdcovid;

import org.apache.spark.api.java.JavaSparkContext;
import com.afjcjsbx.sabdcovid.spark.Spark;
import com.afjcjsbx.sabdcovid.spark.queries.Query1;
import com.afjcjsbx.sabdcovid.spark.queries.Query2;
import com.afjcjsbx.sabdcovid.spark.queries.Query3;

public class StartProcessing {

    public static void main(String[] args) {

        Spark sparkProcessor = new Spark();
        JavaSparkContext sparkContext = sparkProcessor.getSparkContext();

        sparkProcessor.addQuery(new Query1(sparkContext));
        sparkProcessor.addQuery(new Query2(sparkContext));
        sparkProcessor.addQuery(new Query3(sparkContext));

        sparkProcessor.startQueries();

        sparkContext.stop();
        sparkProcessor.close();
    }

}
