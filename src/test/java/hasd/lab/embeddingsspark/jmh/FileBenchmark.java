package hasd.lab.embeddingsspark.jmh;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static hasd.lab.embeddingsspark.util.Constants.PATH_TO_AVRO_EMBEDDINGS_FILE;

@Warmup(iterations = 1, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class FileBenchmark {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class AdditionalCounters {
        private long fileSize;

        @Setup(Level.Iteration)
        public void clean() {
            fileSize = 0;
        }

        public long getFileSize() {
            return fileSize;
        }

        public void setFileSize(long fileSize) {
            this.fileSize = fileSize;
        }
    }


    @Benchmark
    public void measure(AdditionalCounters counters, ExecutionPlan plan) throws IOException {
        counters.setFileSize(getFolderSize(new File(PATH_TO_AVRO_EMBEDDINGS_FILE + plan.getEmbeddingsAmount() + "/" + "embeddings." + plan.getFormat().toLowerCase())));
    }

    private long getFolderSize(File folder) {
        long length = 0;
        File[] files = folder.listFiles();

        for (File file : files) {
            if (file.isFile()) {
                length += file.length();
            } else {
                length += getFolderSize(file);
            }
        }
        return length;
    }
}
