package com.afjcjsbx.sabdcovid;

import com.afjcjsbx.sabdcovid.model.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import com.afjcjsbx.sabdcovid.spark.Spark;
import com.afjcjsbx.sabdcovid.spark.queries.Query1;
import com.afjcjsbx.sabdcovid.spark.queries.Query2;
import com.afjcjsbx.sabdcovid.spark.queries.Query3;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class StartProcessing {

    public static void main(String[] args) throws IOException, URISyntaxException {

        try {
            // Configurazione dell'HDFS
            FileSystem hdfs = Config.configureFileSystem();

            // Eliminazione dei precedenti risultati
            if (hdfs.exists(new Path(Config.OUTPUT_DIR))) {
                hdfs.delete(new Path(Config.OUTPUT_DIR), true);
            }

        }catch (ConnectException e){
            System.out.println("HDFS not connected !");
        }


        // Inizializzazione dello Spark Processor
        Spark sparkProcessor = new Spark();
        JavaSparkContext sparkContext = sparkProcessor.getSparkContext();

        sparkProcessor.addQuery(new Query1(sparkContext));
        sparkProcessor.addQuery(new Query2(sparkContext));
        sparkProcessor.addQuery(new Query3(sparkContext));

        sparkProcessor.startQueries();

        sparkContext.stop();
        sparkProcessor.close();

        // Una volta terminato il processamento stoppiamo Spark per 24 ore
        try {
            TimeUnit.DAYS.sleep(1);
            System.out.println("**Spark paused for 24 hours from now**");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
