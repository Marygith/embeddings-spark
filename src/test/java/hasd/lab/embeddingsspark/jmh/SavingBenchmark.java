package hasd.lab.embeddingsspark.jmh;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


@Fork(2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class SavingBenchmark {
    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Thread)
    public static class BenchmarkState {

        @Setup(Level.Invocation)
        public void doSetup(ExecutionPlan plan) throws IOException, InterruptedException {
            System.out.println("Setup started");
            String pathToClass = "hasd.lab.embeddingsspark.jobs.SaveAvroEmbeddingsTo" + plan.getFormat();
            String compType = plan.getCompressionType().equals("gzip") & plan.getFormat().equals("Orc") ? "zlib" : plan.getCompressionType();
            spark = plan.launchSparkSession(pathToClass, plan.getEmbeddingsAmount(), compType);
            System.out.println("Setup ended");
        }

        @TearDown(Level.Invocation)
        public void doTearDown() {
            System.out.println("TearDown started");
            spark = null;
            System.gc();
            System.out.println("TearDown ended");
        }

        public Process spark;
    }


    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSaving(BenchmarkState state, ExecutionPlan plan) throws InterruptedException {
        plan.launchJob(state.spark);
    }

}