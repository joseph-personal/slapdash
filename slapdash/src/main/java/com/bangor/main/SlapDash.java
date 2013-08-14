package com.bangor.main;

import com.bangor.evaluation.Evaluator;
import com.bangor.empirical.FrequencyTest;
import com.bangor.empirical.SerialTest;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
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

    public static void main(String[] args) {
        try {
            SlapDash slapDashInst = new SlapDash(args);
        } catch (Exception ex) {
            Logger.getLogger(SlapDash.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Constructor creates instance and carries out tests based on input
     *
     * @param args
     * @throws Exception
     */
    public SlapDash(String[] args) throws Exception {
        String test = args[0];
        //TODO: allow different types of this (not int only)
        if (test.equalsIgnoreCase("-F")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            String sEvaluation = args[2];
            double dSignificance = Double.parseDouble(args[3]);
            String sInput = args[4];
            String sOutput = args[5];

            boolean bTestPasses = frequencyTestHandler(iRange, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);

        } else if (test.equalsIgnoreCase("-S")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iLengthOfPattern = Integer.parseInt(args[1]);
            int iRange = Integer.parseInt(args[2]);
            String sEevaluation = args[3];
            double dSignificance = Double.parseDouble(args[4]);
            String sInput = args[5];
            String sOutput = args[6];

            boolean bTestPasses = serialTestHandler(iLengthOfPattern, iRange, dSignificance, sEevaluation, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        }
    }

    private boolean frequencyTestHandler(int iRange, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        FrequencyTest fTest = new FrequencyTest();
        JobConf conf = fTest.test(sInput, sOutput);

        String sLocalOutput = getFileFromHDFS(sOutput, conf);
        int numOfCombinations = getCombinationAmount(iRange, 1).intValue();
//        System.out.println("numOfCombinations = " + numOfCombinations);
        double[] dArrExpected = new double[numOfCombinations];
        for (int i = 0; i < dArrExpected.length; i++) {
            dArrExpected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, sLocalOutput, dArrExpected, dSignificance);

        return evaluator.evaluate();
    }

    private boolean serialTestHandler(int iLengthOfPattern, int iRange, double dSignificance, String sEvaluation, String sInput, String sOutput) throws Exception {

        SerialTest sTest = new SerialTest();
        JobConf conf = sTest.test(iLengthOfPattern, sInput, sOutput);

        String localOutput = getFileFromHDFS(sOutput, conf);
        int numOfCombinations = getCombinationAmount(iRange, iLengthOfPattern).intValue();
        double[] expected = new double[numOfCombinations];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance);

        return evaluator.evaluate();
    }

    private BigInteger getCombinationAmount(int iPossibleObservations, int iPatternLength) {
        BigInteger biNumOfCombinations;

        
//        System.out.println("iPossibleObservations = " + iPossibleObservations);
        BigInteger biPossibleObservationsFactorial = factorial(iPossibleObservations);
//        System.out.println("iPossibleObservationsFactorial = " + biPossibleObservationsFactorial);
        
//        System.out.println("iPatternLength = " + iPatternLength);
        BigInteger biPatternLengthFactorial = factorial(iPatternLength);
//        System.out.println("lPatternLengthFactorial = " + biPatternLengthFactorial);
        
        BigInteger biLastFactorial = factorial(iPossibleObservations - iPatternLength);
//        System.out.println("lLastFactorial = " + biLastFactorial);
        
//        System.out.println(biPossibleObservationsFactorial + "/ ( " + biPatternLengthFactorial + " * " + biLastFactorial + " )");
        biNumOfCombinations = biPossibleObservationsFactorial.divide(biPatternLengthFactorial.multiply(biLastFactorial) );
        
//        System.out.println("iNumOfCombinations = " + lNumOfCombinations);
        return biNumOfCombinations;
    }

    private BigInteger factorial(int iNumToProcess) {
        BigInteger biEndFactorial = BigInteger.valueOf(iNumToProcess);
        for (int i = iNumToProcess-1; i > 0; i--) {
//            System.out.print("\t" + biEndFactorial + " * " + i + " = ");
            biEndFactorial = biEndFactorial.multiply(BigInteger.valueOf(i));
//            System.out.print(biEndFactorial + "\n");
        }

        return biEndFactorial;
    }

    private String getFileFromHDFS(String sFilePath, JobConf conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(sFilePath + "/part-00000");
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
