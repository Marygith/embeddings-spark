package hasd.lab.embeddingsspark.service;

import org.apache.spark.sql.SparkSession;

public class SparkSessionBuilder {

    public static SparkSession create() {
        return SparkSession
                .builder()
                .config("spark.master", "local[*]")
                .appName("avro2orc&parquet")
                .config("spark.sql.shuffle.partitions", "800")
                .getOrCreate();
    }
}
