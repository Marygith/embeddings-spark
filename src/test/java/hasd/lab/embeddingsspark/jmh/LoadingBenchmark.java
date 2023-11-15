package hasd.lab.embeddingsspark.jmh;

import hasd.lab.embeddingsspark.util.Constants;
import org.apache.spark.launcher.SparkLauncher;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

    @Fork(value = 1, warmups = 1)
    @State(Scope.Benchmark)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
//    @Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 1, time = 20, timeUnit = TimeUnit.MILLISECONDS)
    public class LoadingBenchmark {

        public static void main(String[] args) throws Exception {
            org.openjdk.jmh.Main.main(args);
        }
        @Benchmark
        @BenchmarkMode(Mode.SingleShotTime)
        public void testLoading(ExecutionPlan plan) throws IOException, InterruptedException {
            plan.cleanDirectory(new File(Constants.PATH_TO_AVRO_EMBEDDINGS_FILE + plan.getEmbeddingsAmount()));
            String pathToClass = "hasd.lab.embeddingsspark.jobs.LoadEmbeddingsFrom" + plan.getFormat();
            plan.launchSpark(pathToClass, plan.getEmbeddingsAmount());
        }
    }
