package com.bangor.main;

import com.bangor.general.Evaluator;
import com.bangor.stat.FrequencyTest;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author Joseph W Plant
 */
public class SlapDash {

    public static void main(String[] args) throws Exception {
        String test = args[0];
        String evaluation = args[1];
        double significance = Double.parseDouble(args[2]);
        String input = args[3];
        String output = args[4];
        
        SlapDash slapDashInst = new SlapDash();

        //TODO: allow different types of this (not int only)
        if (test.equalsIgnoreCase("-F")) {
            FrequencyTest fTest = new FrequencyTest();
            JobConf conf = fTest.test(input, output);
            
            String localOutput = slapDashInst.getFileFromHDFS(output, conf);
            double[] expected = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
            Evaluator evaluator = new Evaluator(evaluation, localOutput, expected, significance);
            
            System.err.println("Pass: " + evaluator.evaluate());
            
        } else if (test.equalsIgnoreCase("-S")) {
        }
    }
    
    

    private String getFileFromHDFS(String string_filePath, JobConf conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(string_filePath + "/part-00000");
        if (!fileSystem.exists(path)) {
            throw new IOException("File path does not exist: \n" + string_filePath);
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
