package model;

import lombok.Cleanup;
import lombok.NonNull;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

public class Config extends HashMap<String, Object> implements Serializable {

    public final static String DATA_STORE = "data-store";
    public final static String DEFAULT_DATA_STORE = "hdfs://hdfs-namenode:54310";

    public final static String SPARK_MASTER = "spark-master";
    //public final static String DEFAULT_SPARK_MASTER = "spark://spark:4040";
    public final static String DEFAULT_SPARK_MASTER = "http://PC-Mauro:4040";

    public final static String APP_NAME = "app-name";
    public final static String DEFAULT_APP_NAME = "SparkQueries";

    public final static String SPARK_LOG_LEVEL = "spark-log-level";
    public final static String DEFAULT_SPARK_LOG_LEVEL = "ERROR";

    public final static String SERIALIZER_KEY = "serializer-key";
    public final static String DEFAULT_SERIALIZER_KEY = "spark.serializer";

    public final static String SERIALIZER_VALUE = "serializer-value";
    public final static String DEFAULT_SERIALIZER_VALUE = "org.apache.spark.serializer.KryoSerializer";

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
