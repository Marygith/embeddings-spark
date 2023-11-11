package hasd.lab.embeddingsspark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class AvroEmbeddingsLoader {

    private final SparkSession spark;

    public AvroEmbeddingsLoader() {
        spark = SparkSession
                .builder()
                .appName("avro2orc&parquet")
                .getOrCreate();
    }

    public void loadEmbeddingsFromFile() {
        Dataset<Row> embeddingsDf = spark.read().format("avro").load("examples/src/main/resources/users.avro");
        String[] cols = embeddingsDf.columns();
        Arrays.stream(cols).forEach(System.out::println);
    }
}
