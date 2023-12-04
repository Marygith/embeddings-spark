package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import hasd.lab.embeddingsspark.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SaveAvroEmbeddingsToParquet {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionBuilder.create();
        Dataset<Row> embeddingsDf = spark.read().format("avro")
                .load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE
                        + args[0] + Constants.FILE_NAME).repartition(2000);

        embeddingsDf.write().mode("overwrite")
                .format("parquet").option("compression", args[1])
                .save(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE
                        + args[0] + args[1] + "/" + "embeddings.parquet");
        spark.stop();
    }
}
