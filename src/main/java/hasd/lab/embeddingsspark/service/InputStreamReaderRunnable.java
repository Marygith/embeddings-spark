package hasd.lab.embeddingsspark.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
    }

    public void run() {
        System.out.println("InputStream " + name + ":");
        try {
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
