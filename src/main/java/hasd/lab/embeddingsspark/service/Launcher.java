package hasd.lab.embeddingsspark.service;

import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
    public static void main(String[] args) throws Exception {
        Process spark = new SparkLauncher()
                .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass("hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsToOrc")
                .setMaster("local")
                .setVerbose(true)
                .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
//                .addSparkArg("qwerty/")
                .addAppArgs("10/")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .launch();
        spark.waitFor();
    }
}
