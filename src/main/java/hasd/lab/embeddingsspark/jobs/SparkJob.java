package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;

public abstract class SparkJob {

    private SparkSession spark;
    protected abstract String getFormat();
    protected SparkSession createSparkSession() {
        return SparkSessionBuilder.create();
    }
}
