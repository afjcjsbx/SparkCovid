package com.afjcjsbx.sabdcovid.model;

import lombok.Cleanup;
import lombok.NonNull;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Config extends HashMap<String, Object> implements Serializable {

    // Parametri dei KMeans
    public static int NUM_CLUSTERS = 4;
    public static int NUM_ITERATIONS = 20;

    public static int WEEK_OFFSET_QUERY_1 = 0; // Sostituire con 9 per far coincidere la settimana di inizio del dataset con la prima settimana
    public static int WEEK_OFFSET_QUERY_2 = 0; // Sostituire con 9 per far coincidere la settimana di inizio del dataset con la prima settimana

    public final static String DATA_STORE = "master";
    public final static String DEFAULT_DATA_STORE = "hdfs://master:54310";

    public final static String REDIS_HOSTNAME = "redis-hostname";
    public final static String DEFAULT_REDIS_HOSTNAME = "redis";


    public static String PATH_DATASET_1 = DEFAULT_DATA_STORE + "/csv_datasets/dataset1.csv";
    public static String PATH_DATASET_2 = DEFAULT_DATA_STORE + "/csv_datasets/dataset2.csv";
    public static String PATH_COUNTRY_CONTINENT = DEFAULT_DATA_STORE + "/csv_datasets/country_continent.csv";

    public static String OUTPUT_DIR = DEFAULT_DATA_STORE + "/results/";

    public static String PATH_RESULT_QUERY_1 = OUTPUT_DIR + "resultQuery1";
    public static String PATH_RESULT_QUERY_2 = OUTPUT_DIR + "resultQuery2";
    public static String PATH_RESULT_QUERY_3_MLIB = OUTPUT_DIR + "resultQuery3Mlib";
    public static String PATH_RESULT_QUERY_3_NAIVE = OUTPUT_DIR + "resultQuery3Naive";


    public final static String SPARK_MASTER = "com.afjcjsbx.sabdcovid.spark-master";
    //public final static String DEFAULT_SPARK_MASTER = "com.afjcjsbx.sabdcovid.spark://com.afjcjsbx.sabdcovid.spark:4040";
    public final static String DEFAULT_SPARK_MASTER = "http://PC-Mauro:4040";

    public final static String APP_NAME = "app-name";
    public final static String DEFAULT_APP_NAME = "SparkQueries";

    public final static String SPARK_LOG_LEVEL = "com.afjcjsbx.sabdcovid.spark-log-level";
    public final static String DEFAULT_SPARK_LOG_LEVEL = "ERROR";

    public final static String SERIALIZER_KEY = "serializer-key";
    public final static String DEFAULT_SERIALIZER_KEY = "com.afjcjsbx.sabdcovid.spark.serializer";

    public final static String SERIALIZER_VALUE = "serializer-value";
    public final static String DEFAULT_SERIALIZER_VALUE = "org.apache.com.afjcjsbx.sabdcovid.spark.serializer.KryoSerializer";

    public static final String PROPERTIES_FILENAME = "properties-filename";
    public static final String DEFAULT_PROPERTIES_FILENAME = "conf.properties";


    private static class SingletonContainer {
        private final static Config INSTANCE = new Config();
    }

    protected Config() {
        put(DATA_STORE, DEFAULT_DATA_STORE);
        put(SPARK_MASTER, DEFAULT_SPARK_MASTER);
        put(APP_NAME, DEFAULT_APP_NAME);
        put(SPARK_LOG_LEVEL, DEFAULT_SPARK_LOG_LEVEL);
        put(REDIS_HOSTNAME, DEFAULT_REDIS_HOSTNAME);
        put(SERIALIZER_KEY, DEFAULT_SERIALIZER_KEY);
        put(SERIALIZER_VALUE, DEFAULT_SERIALIZER_VALUE);

        put(PROPERTIES_FILENAME, DEFAULT_PROPERTIES_FILENAME);

    }

    public static Config getInstance() {
        return SingletonContainer.INSTANCE;
    }

    public void load() throws IOException {
        load(Config.class
                .getClassLoader()
                .getResourceAsStream(getConfigurationFilename())
        );
    }

    public void load(@NonNull String configFile) throws IOException {
        setConfigurationFilename(configFile);
        load(new FileInputStream(configFile));
    }

    public void load(@NonNull InputStream confInputStream) throws IOException {
        @Cleanup InputStream configInputStream = confInputStream;
        @Cleanup BufferedInputStream configBufferedInputStream = new BufferedInputStream(configInputStream);
        Properties properties = new Properties();
        properties.load(configBufferedInputStream);

        checkNullAndPutString(DATA_STORE, properties.get(DATA_STORE));
        checkNullAndPutString(SPARK_MASTER, properties.get(SPARK_MASTER));
        checkNullAndPutString(APP_NAME, properties.get(APP_NAME));
        checkNullAndPutString(SPARK_LOG_LEVEL, properties.get(SPARK_LOG_LEVEL));
        checkNullAndPutString(REDIS_HOSTNAME, properties.get(REDIS_HOSTNAME));
        //checkNullAndPutString(SERIALIZER_KEY, properties.get(SERIALIZER_KEY));
        //checkNullAndPutString(SERIALIZER_VALUE, properties.get(SERIALIZER_VALUE));

        put(PROPERTIES_FILENAME, DEFAULT_PROPERTIES_FILENAME);
    }

    private void checkNullAndPutString(String key, Object value) {
        if (value != null) {
            put(key, value);
        }
    }

    private void checkNullAndPutBool(String key, Object value) {
        if (value != null) {
            put(key, Boolean.valueOf((String) value));
        }
    }


    private void checkNullAndPutInteger(String key, Object value) {
        if (value != null) {
            put(key, Integer.valueOf((String) value));
        }
    }

    public String getDataStore() {
        return (String) get(DATA_STORE);
    }

    public String getSparkMaster() {
        return (String) get(SPARK_MASTER);
    }

    public String getRedisHostname() {
        return (String) get(REDIS_HOSTNAME);
    }

    public String getAppName() {
        return (String) get(APP_NAME);
    }

    public String getSparkLogLevel() {
        return (String) get(SPARK_LOG_LEVEL);
    }


    public String getSerializerKey() {
        return (String) get(SERIALIZER_KEY);
    }

    public String getSerializerValue() {
        return (String) get(SERIALIZER_VALUE);
    }




    public void setConfigurationFilename(@NonNull String configurationFile) {
        put(PROPERTIES_FILENAME, configurationFile);
    }

    public String getConfigurationFilename() {
        return (String) get(PROPERTIES_FILENAME);
    }

    /**
     * To configure the file system as per the Hadoop Configuration
     * @return hadoop file system instance
     */
    public static FileSystem configureFileSystem() {

        Configuration configuration = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new URI(DEFAULT_DATA_STORE), configuration);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

        return hdfs;
    }



    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("### \t\tApplication's properties\t\t\n");
        keySet().forEach(k -> stringBuilder
                .append("# \t")
                .append(k)
                .append(":\t")
                .append(get(k))
                .append("\n"));
        return stringBuilder.toString();
    }

}
