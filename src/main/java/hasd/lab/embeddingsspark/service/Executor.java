package hasd.lab.embeddingsspark.service;

public class Executor {

    public static void main(String[] args) {
        AvroEmbeddingsLoader loader = new AvroEmbeddingsLoader();
        loader.loadEmbeddingsFromFile();
    }
}
