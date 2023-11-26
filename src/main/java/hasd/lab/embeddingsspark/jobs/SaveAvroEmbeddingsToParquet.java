package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import hasd.lab.embeddingsspark.util.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class SaveAvroEmbeddingsToParquet {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSessionBuilder.create();
        Dataset<Row> embeddingsDf = spark.read().format("avro").load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE + args[0] + Constants.FILE_NAME);
        embeddingsDf.write().mode("overwrite").format("parquet").save(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE + args[0] + "embeddings.parquet");
        spark.stop();
    }
}
