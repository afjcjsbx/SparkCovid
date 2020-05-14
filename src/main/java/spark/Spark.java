package spark;

import lombok.NonNull;
import model.Config;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class Spark extends AbstractSpark {

    private final static Config config = Config.getInstance();

    @Override
    protected @NonNull SparkConf defineSparkConf() {
        return new SparkConf()
                //.setMaster(config.getSparkMaster())
                .setMaster("local")
                .setAppName(config.getAppName())
                .set(config.getSerializerKey(), config.getSerializerValue())
                .registerKryoClasses(new Class[]{GenericData.class, GenericData.Record.class});
    }

    @Override
    protected @NonNull JavaSparkContext defineSparkContext() {
        JavaSparkContext sparkContext = new JavaSparkContext(getSparkConf());
        sparkContext.setLogLevel(config.getSparkLogLevel());
        return sparkContext;
    }

}
