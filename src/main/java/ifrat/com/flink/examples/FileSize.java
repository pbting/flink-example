package ifrat.com.flink.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileSize {

    static int size = 0;

    public static void main(String[] args) throws Exception {
        String file = "/Users/ifrati/Documents/_cec/_project_code/cls-service";
        fileSize(new File(file));

        System.err.println(size);
    }

    public static void fileSize(File file) throws IOException {

        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    fileSize(f);
                } else if (f.getName().endsWith(".go")) {
                    BufferedReader reader = new BufferedReader(new FileReader(f));
                    String l;
                    while ((l = reader.readLine()) != null) {
                        size++;
                    }
                }
            }
        }
    }
}
