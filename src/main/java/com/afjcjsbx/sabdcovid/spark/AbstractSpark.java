package com.afjcjsbx.sabdcovid.spark;

import com.clearspring.analytics.util.Lists;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.afjcjsbx.sabdcovid.spark.queries.IQuery;

import java.util.List;

@Getter
@Setter
public abstract class AbstractSpark {

    protected final SparkConf sparkConf;
    protected final JavaSparkContext sparkContext;

    protected List<IQuery> sparkQueries;

    protected AbstractSpark() {
        this.sparkQueries = Lists.newArrayList();
        this.sparkConf = defineSparkConf();
        this.sparkContext = defineSparkContext();
    }

    protected abstract @NonNull SparkConf defineSparkConf();

    protected abstract @NonNull JavaSparkContext defineSparkContext();

    public void close() {
        this.sparkContext.close();
    }

    public void addQuery(IQuery sparkQuery) {
        this.sparkQueries.add(sparkQuery);
    }

    public void removeQuery(IQuery sparkQuery) {
        this.sparkQueries.remove(sparkQuery);
    }

    public void removeQuery(int queryPosition) {
        this.sparkQueries.remove(queryPosition);
    }

    public void removeQueries() {
        this.sparkQueries.clear();
    }

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    public void startQuery(IQuery sparkQuery) {
        sparkQuery.load();
        sparkQuery.execute();
        sparkQuery.store();
    }

    public void startQuery(int queryPosition) {
        startQuery(this.sparkQueries.get(queryPosition));
    }

    public void startQueries() {
        for (IQuery sparkQuery : this.sparkQueries) {
            startQuery(sparkQuery);
        }
    }

}
