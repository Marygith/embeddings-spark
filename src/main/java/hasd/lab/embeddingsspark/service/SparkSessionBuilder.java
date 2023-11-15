package hasd.lab.embeddingsspark.service;

import hasd.lab.embeddingsspark.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkSessionBuilder {


    public static SparkSession create() {
        return SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("avro2orc&parquet")
                .getOrCreate();
    }
    private final SparkSession spark;

    public SparkSessionBuilder() {
        spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("avro2orc&parquet")
                .getOrCreate();
    }

    public void close() {
        spark.close();
    }

    public void load() {
        Dataset<Row> embeddingsDf = spark.read().format("avro").load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE);
        String[] cols = embeddingsDf.columns();
        Arrays.stream(cols).forEach(System.out::println);
        embeddingsDf.write().format("parquet").save("embeddings.parquet");
        embeddingsDf.write().format("orc").save("embeddings.orc");
        spark.stop();
    }
    public Dataset<Row> loadEmbeddingsFromFile() {
        return spark.read().format("avro").load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE);
    }
}
