package hasd.lab.embeddingsspark.jobs;

import hasd.lab.embeddingsspark.service.SparkSessionBuilder;
import hasd.lab.embeddingsspark.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadEmbeddingsFromParquet extends SparkJob {

    private static String format;

    public LoadEmbeddingsFromParquet() {
        format = getFormat();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSessionBuilder.create();
        Dataset<Row> embeddingsDf = spark.read().format(format).load(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE + args[0] + "embeddings.orc");
        System.out.println(embeddingsDf.count());
        spark.stop();
    }

    @Override
    protected String getFormat() {
        return "parquet";
    }
}
