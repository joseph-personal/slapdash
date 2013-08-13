package com.bangor.main;

import com.bangor.evaluation.Evaluator;
import com.bangor.empirical.FrequencyTest;
import com.bangor.empirical.SerialTest;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author Joseph W Plant
 */
public class SlapDash {

    public static void main(String[] args){
        try {
            SlapDash slapDashInst = new SlapDash(args);
        } catch (Exception ex) {
            Logger.getLogger(SlapDash.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Constructor creates instance and carries out tests based on input
     * @param args
     * @throws Exception 
     */
    public SlapDash(String[] args) throws Exception{
        String test = args[0];
         //TODO: allow different types of this (not int only)
        if (test.equalsIgnoreCase("-F")) {

            //GET REST OF PARAMETER ARGUMENTS
            String evaluation = args[1];
            double significance = Double.parseDouble(args[2]);
            String input = args[3];
            String output = args[4];
            
            FrequencyTest fTest = new FrequencyTest();
            JobConf conf = fTest.test(input, output);

            String localOutput = getFileFromHDFS(output, conf);
            double[] expected = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
            Evaluator evaluator = new Evaluator(evaluation, localOutput, expected, significance);

            System.err.println("Pass: " + evaluator.evaluate());

        } else if (test.equalsIgnoreCase("-S")) {
            
            //GET REST OF PARAMETER ARGUMENTS
            int lengthOfPattern = Integer.parseInt(args[1]);
            String evaluation = args[2];
            double significance = Double.parseDouble(args[3]);
            String input = args[4];
            String output = args[5];
            
            boolean bTestPasses = serialTestHandler(lengthOfPattern, evaluation, significance, input, output);
            System.err.println("Pass: " + bTestPasses);
        }
    }
    
    private boolean serialTestHandler(int lengthOfPattern, String evaluation, double significance, String input, String output) throws Exception{
        
            
            SerialTest sTest = new SerialTest();
            JobConf conf = sTest.test(lengthOfPattern, input, output);

            String localOutput = getFileFromHDFS(output, conf);
            //TODO: make dynamic way of getting this
            double[] expected = new double[100];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = 1.0;
            }
            Evaluator evaluator = new Evaluator(evaluation, localOutput, expected, significance);

            return evaluator.evaluate();
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
