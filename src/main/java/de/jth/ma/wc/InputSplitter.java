package de.jth.ma.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jth on 10/6/15.
 */
class InputSplitter {
    private final Path inputPath;
    private final List<FileStatus> stats = new ArrayList<>();

    InputSplitter(Path inputPath) {
        this.inputPath = inputPath;
    }

    void stat() throws IOException {
        final FileSystem fs = FileSystem.get(new Configuration());
        final FileStatus status[] = fs.listStatus(inputPath);

        for (int i=0;i<status.length; i++){
            stats.add(status[i]);
        }
    }

    List<FileStatus> getStats() {
        return stats;
    }

}
