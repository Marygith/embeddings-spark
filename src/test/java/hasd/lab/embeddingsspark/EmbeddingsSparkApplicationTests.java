package hasd.lab.embeddingsspark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddingsSparkApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void saveToOrcTest() {
        File directory = new File("orc");
        directory.mkdir();
        cleanDirectory(directory);
        assertTrue(checkThatDirectoryIsEmpty("orc"));
        assertTrue(launchSpark("hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsToOrc"));
        assertFalse(checkThatDirectoryIsEmpty("orc"));
    }

    @Test
    void saveToParquetTest() {
        File directory = new File("parquet");
        directory.mkdir();
        cleanDirectory(directory);
        assertTrue(checkThatDirectoryIsEmpty("parquet"));
        assertTrue(launchSpark("hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsToParquet"));
        assertFalse(checkThatDirectoryIsEmpty("parquet"));
    }

    @Test
    void loadFromParquetTest() {
        assertTrue(launchSpark("hasd.lab.embeddingsspark.jobs.LoadEmbeddingsFromParquet"));
    }

    @Test
    void loadFromOrcTest() {
        assertTrue(launchSpark("hasd.lab.embeddingsspark.jobs.LoadEmbeddingsFromOrc"));
    }

    @Test
    public void testLoading() throws IOException, InterruptedException {
        String pathToClass = "hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsToOrc";
        launchSpark(pathToClass, 10);
    }


    private boolean launchSpark(String mainClass) {
        try {
            Process spark = new SparkLauncher()
                    .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                    .setMainClass(mainClass)
                    .setMaster("local")
                    .setVerbose(true)
                    .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .launch();
            spark.waitFor();
        } catch (IOException | InterruptedException e) {
            return false;
        }
        return true;
    }

    private boolean checkThatDirectoryIsEmpty(String directoryName) {
        try (Stream<Path> stream = Files.list(Path.of(directoryName))) {
            return stream.findAny().isEmpty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void cleanDirectory(File directory) {
        try {
            FileUtils.cleanDirectory(directory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void launchSpark(String mainClass, int embeddingsAmount) throws InterruptedException, IOException {
        Process spark = new SparkLauncher()
                .setAppResource("/home/maria/IdeaProjects/embeddings-spark/target/embeddings-spark-0.0.1-SNAPSHOT.jar")
                .setMainClass(mainClass)
                .setMaster("local[*]")
                .addSparkArg("--packages", "org.apache.spark:spark-avro_2.12:3.5.0")
                .addAppArgs(embeddingsAmount + "/", "snappy")
                .setConf(SparkLauncher.DRIVER_MEMORY, "10g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "3g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "4")
                .launch();
        spark.waitFor();
    }

}
