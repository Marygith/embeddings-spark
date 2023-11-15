package hasd.lab.embeddingsspark.jmh.read;

import hasd.lab.embeddingsspark.jmh.ExecutionPlan;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
//    @Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 20, timeUnit = TimeUnit.MILLISECONDS)
public class SavingBenchmark {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void testSaving(ExecutionPlan plan) throws IOException, InterruptedException {
        String pathToClass = "hasd.lab.embeddingsspark.jobs.SaveEmbeddingsTo" + plan.getFormat();
        plan.launchSpark(pathToClass, plan.getEmbeddingsAmount());
    }
}