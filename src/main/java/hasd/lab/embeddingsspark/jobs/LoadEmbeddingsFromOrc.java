package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import hasd.lab.embeddingsspark.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadEmbeddingsFromOrc {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionBuilder.create();
        Dataset<Row> embeddingsDf = spark.read().format("orc").load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE + args[0] + "embeddings.orc");
        System.out.println(embeddingsDf.count());
        spark.stop();
    }
}
