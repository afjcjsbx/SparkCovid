import model.Config;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import spark.Spark;
import spark.queries.Query1;
import spark.queries.Query2;
import spark.queries.Query3;

import java.io.IOException;

public class StartProcessing {

    //private static final Logger log = LogManager.getLogger(StartProcessing.class);

    // File di configurazione
    private final static Config config = Config.getInstance();

    public static void main(String[] args) {
        try {
            if (args.length != 0) config.load(args[0]);
            else config.load();
        } catch (IOException e) {
            //log.error("Error while reading configuration file");
            e.printStackTrace();
        }
        //log.info(config.toString());

        Spark sparkProcessor = new Spark();
        JavaSparkContext sparkContext = sparkProcessor.getSparkContext();

        //sparkProcessor.addQuery(new Query1(sparkContext));
        //sparkProcessor.addQuery(new Query2(sparkContext));
        sparkProcessor.addQuery(new Query3(sparkContext));

        sparkProcessor.startQueries();

        sparkContext.stop();
        sparkProcessor.close();
    }

}
