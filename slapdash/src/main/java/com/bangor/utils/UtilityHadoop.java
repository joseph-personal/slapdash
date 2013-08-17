package com.bangor.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author Joseph W Plant
 */
public class UtilityHadoop {

    public static String getFileFromHDFS(String sFilePath, Job job) throws IOException {
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());

        Path path = new Path(sFilePath);
        if (!fileSystem.exists(path)) {
            throw new IOException("File path does not exist: \n" + sFilePath);
        }

        FSDataInputStream inFS = fileSystem.open(path);

        String fileName = "tmp_file";

        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(fileName)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = inFS.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        inFS.close();
        out.close();
        fileSystem.close();

        return fileName;
    }
}
