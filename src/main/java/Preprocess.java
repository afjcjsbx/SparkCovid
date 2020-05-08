
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import utils.CSVReaderInJava;
import utils.Covid1Data;
import utils.FileUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Preprocess {

    String pathFileWeatherDescription = "hdfs://127.0.0.1:54310/dpc-covid19-ita-andamento-nazionale.csv";


    public static void main(String[] args) throws IOException, URISyntaxException {


        //1. Get the instance of COnfiguration
        Configuration configuration = new Configuration();
        //2. Create an InputStream to read the data from local file
        InputStream inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/shakespeare.txt"));
        //3. Get the HDFS instance
        FileSystem hdfs = FileSystem.get(new URI("hdfs://127.0.0.1:54310"), configuration);
        //4. Open a OutputStream to write the data, this can be obtained from the FileSytem
        OutputStream outputStream = hdfs.create(new Path("hdfs://127.0.0.1:54310/Hadoop_File.txt"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.println("....");
                    }
                });
        try
        {
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        }
        finally
        {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }

}