package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import hasd.lab.embeddingsspark.util.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

public class SaveAvroEmbeddingsToOrc {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionBuilder.create();
        Dataset<Row> embeddingsDf = spark.read().format("avro")
                .load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE
                        + args[0] + "embeddings.avro").repartition(2000);

        embeddingsDf.write().mode("overwrite")
                .format("orc").option("compression", args[1])
                .save(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE
                        + args[0] + args[1] + "/" + "embeddings.orc");
        spark.stop();
    }
}
