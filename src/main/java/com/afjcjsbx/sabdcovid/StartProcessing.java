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
import java.net.URI;
import java.net.URISyntaxException;

public class StartProcessing {

    public static void main(String[] args) throws IOException, URISyntaxException {

        // Get datastore filesystem
        FileSystem hdfs = Config.configureFileSystem();

        // Delete output if exists
        if (hdfs.exists(new Path(Config.OUTPUT_DIR))){
            hdfs.delete(new Path(Config.OUTPUT_DIR), true);
        }


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
