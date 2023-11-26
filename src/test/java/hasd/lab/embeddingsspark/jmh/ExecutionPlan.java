package hasd.lab.embeddingsspark.jmh;

import org.apache.commons.io.FileUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;

@State(Scope.Benchmark)
public class ExecutionPlan {
    @Param({"Orc", "Parquet"})
    private String format;

    @Param({"10", "100", "1000", "10000"})
    private int embeddingsAmount;

    public String getFormat() {
        return format;
    }

    public int getEmbeddingsAmount() {
        return embeddingsAmount;
    }


    public void launchSpark(String mainClass, int embeddingsAmount) throws InterruptedException, IOException {
        Process spark = new SparkLauncher()
                .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass(mainClass)
                .setMaster("local")
                .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
                .addAppArgs(embeddingsAmount + "/")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .launch();
        spark.waitFor();
    }
}
