package hasd.lab.embeddingsspark.service;


import org.apache.spark.launcher.SparkLauncher;

import java.io.File;

public class Launcher {

    public static void main(String[] args) throws Exception {
        Process spark = new SparkLauncher()
                .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass("hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsToOrc")
                .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
                .addAppArgs("10000/", "zlib")
                .setConf(SparkLauncher.DRIVER_MEMORY, "10g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "3g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "4")
                .launch();

        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        spark.waitFor();
    }
}
