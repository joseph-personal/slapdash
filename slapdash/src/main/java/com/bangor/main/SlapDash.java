package com.bangor.main;

import com.bangor.evaluation.Evaluator;
import com.bangor.empirical.FrequencyTest;
import com.bangor.empirical.GapTest;
import com.bangor.empirical.PokerTest;
import com.bangor.empirical.RunsTest;
import com.bangor.empirical.SerialTest;
import com.bangor.utils.UtilityHadoop;
import com.bangor.utils.UtilityMath;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.mapreduce.Job;

/**
 * Class for command line tool api
 *
 * @author Joseph W Plant
 */
public class SlapDash {

    String sFileName = "part-r-00000";

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
        if (test.equalsIgnoreCase("-F")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int idegree = Integer.parseInt(args[2]);
            String sEvaluation = args[3];
            double dSignificance = Double.parseDouble(args[4]);
            String sInput = args[5];
            String sOutput = args[6];

            boolean bTestPasses = frequencyTestHandler(iRange, idegree, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);

        } else if (test.equalsIgnoreCase("-S")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iLengthOfPattern = Integer.parseInt(args[1]);
            int iRange = Integer.parseInt(args[2]);
            int idegree = Integer.parseInt(args[3]);
            String sEevaluation = args[4];
            double dSignificance = Double.parseDouble(args[5]);
            String sInput = args[6];
            String sOutput = args[7];

            boolean bTestPasses = serialTestHandler(iLengthOfPattern, iRange, idegree, dSignificance, sEevaluation, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-R")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int idegree = Integer.parseInt(args[2]);
            String sEevaluation = args[3];
            double dSignificance = Double.parseDouble(args[4]);
            String sInput = args[5];
            String sOutput = args[6];

            boolean bTestPasses = runTestHandler(iRange, idegree, dSignificance, sEevaluation, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-G")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int idegree = Integer.parseInt(args[2]);
            float dMinimumRange = Float.parseFloat(args[3]);
            float dMaximumRange = Float.parseFloat(args[4]);
            String sEevaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = gapTestHandler(iRange, idegree, dMinimumRange, dMaximumRange, sEevaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        }
    }

    /**
     * Handler for Frequency Test
     *
     * @param iRange Range of generator
     * @param iDegree the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean frequencyTestHandler(int iRange, int iDegree, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        FrequencyTest fTest = new FrequencyTest();
        Job conf = fTest.test(sInput, sOutput);

        String sLocalOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange, 1, true, true).intValue();
//        System.out.println("numOfCombinations = " + numOfCombinations);
        double[] dArrExpected = new double[numOfCombinations];
        for (int i = 0; i < dArrExpected.length; i++) {
            dArrExpected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, sLocalOutput, dArrExpected, dSignificance, iRange, iDegree);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDegree the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean serialTestHandler(int iLengthOfPattern, int iRange, int iDegree, double dSignificance, String sEvaluation, String sInput, String sOutput) throws Exception {

        SerialTest sTest = new SerialTest();
        Job conf = sTest.test(iLengthOfPattern, sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);
        System.out.println("***\t\tiRange = " + iRange);
        System.out.println("***\t\tiLengthOfPattern = " + iLengthOfPattern);
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange, iLengthOfPattern, true, true).intValue();
        double[] expected = new double[numOfCombinations];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDegree);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDegree the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean runTestHandler(int iRange, int iDegree, double dSignificance, String sEvaluation, String sInput, String sOutput) throws Exception {

        RunsTest gTest = new RunsTest();
        Job conf = gTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iRange];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDegree);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDegree the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean gapTestHandler(int iRange, int iDegree, float fMinimumRange, float fMaximumRange, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        GapTest gTest = new GapTest(fMinimumRange, fMaximumRange);
        Job conf = gTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iRange];
        expected[0] = (fMaximumRange - fMinimumRange) / iRange;
        //TODO: Check this with Ryan
        for (int i = 1; i < expected.length; i++) {
//            expected[i] = 1.0;
            expected[i] = Math.pow(expected[0] * (1-expected[0]), i);
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDegree);

        return evaluator.evaluate();
    }

    /**
     * Handler for Poker Test
     *
     * @param iRange Range of generator
     * @param iDegree the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean pokerTestHandler(int iRange, int iDegree, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        PokerTest pTest = new PokerTest();
        Job conf = pTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iRange];
        //TODO: Calculate the correct probability amount (in Knuths book)
        for (int i = 1; i < expected.length; i++) {
//            expected[i] = 1.0;
            expected[i] = Math.pow(expected[0] * (1-expected[0]), i);
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDegree);

        return evaluator.evaluate();
    }
}
