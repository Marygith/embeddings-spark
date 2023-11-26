package hasd.lab.embeddingsspark.jmh;

import hasd.lab.embeddingsspark.jmh.ExecutionPlan;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class SavingBenchmark {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSaving(ExecutionPlan plan) throws IOException, InterruptedException {
        String pathToClass = "hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsTo" + plan.getFormat();
        plan.launchSpark(pathToClass, plan.getEmbeddingsAmount());
    }
}