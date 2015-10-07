package de.jth.ma.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by jth on 10/6/15.
 */
public class Mapper {
    private final Path inputFile;
    private final String outputPath;
    private final int id;
    private final Map<String, Long> counts = new HashMap<>();

    Mapper(Path inputFile, String outputPath, int id) {
        this.inputFile = inputFile;
        this.outputPath = outputPath;
        this.id = id;
    }

    private void writeResult(FileSystem fs) throws IOException {
        Path out = new Path(outputPath + File.separator + id);
        final FSDataOutputStream os = fs.create(out, true);

        Iterator it = counts.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            String result = pair.getKey() + "\t" + pair.getValue() + "\n";
            os.write(result.getBytes());
            it.remove(); // avoids a ConcurrentModificationException
        }

        os.close();
    }

    void exec() {
        try{
            final FileSystem fs = FileSystem.get(new Configuration());
            final FSDataInputStream is = fs.open(inputFile);
            final BufferedReader br = new BufferedReader(new InputStreamReader(is.getWrappedStream()));
            String line = br.readLine();

            while (line != null){
                String[] words = line.split("\\s+");
                for (String word : words) {
                    if (!counts.containsKey(word)) {
                        counts.put(word, new Long(1));
                    } else {
                        Long tmp = counts.get(word);
                        counts.put(word, tmp + 1);
                    }
                }
                line = br.readLine();
            }
            // TODO: Remove non printable characters. Do that while reading to avoid multiple spaces, tabs and so on. But ok for now.
            counts.remove("\t");
            //System.out.println("Counted words, closing " + inputFile.getName());
            is.close();
            System.out.println("Writing results");
            Thread.sleep(1000); // prolong tasks artifically...
            writeResult(fs);
        }catch(Exception e){
            throw new RuntimeException("Could not open " + inputFile.getName() + ": " + e.getMessage());
        }
    }

    public static void main(String args[]) {
        final Mapper mapTask = new Mapper(new Path(args[0]), args[1], Integer.valueOf(args[2]));
        mapTask.exec();
    }
}
