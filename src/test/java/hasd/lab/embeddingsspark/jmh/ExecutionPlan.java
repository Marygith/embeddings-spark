package hasd.lab.embeddingsspark.jmh;

import hasd.lab.embeddingsspark.service.InputStreamReaderRunnable;
import org.apache.spark.launcher.SparkLauncher;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

@State(Scope.Benchmark)
public class ExecutionPlan {
    @Param({"Orc", "Parquet"})
    private String format;

    @Param({"100", "1000", "10000"})
    private int embeddingsAmount;

    @Param({"snappy", "gzip"})
    private String compressionType;

    public String getFormat() {
        return format;
    }

    public int getEmbeddingsAmount() {
        return embeddingsAmount;
    }

    public String getCompressionType() {
        return compressionType;
    }


    public Process launchSparkSession(String mainClass, int embeddingsAmount, String compressionType) throws InterruptedException, IOException {
        return new SparkLauncher()
                .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass(mainClass)
                .setMaster("local[*]")
                .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
                .addAppArgs(embeddingsAmount + "/", compressionType)
                .setConf(SparkLauncher.DRIVER_MEMORY, "10g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "3g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "4")
                .launch();
    }

    public void launchJob(Process spark) throws InterruptedException {
        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();
        spark.waitFor();
    }
}
