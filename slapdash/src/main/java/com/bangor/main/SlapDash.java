package com.bangor.main;

import com.bangor.empirical.CouponTest;
import com.bangor.evaluation.Evaluator;
import com.bangor.empirical.FrequencyTest;
import com.bangor.empirical.GapTest;
import com.bangor.empirical.PokerTest;
import com.bangor.empirical.RunsTest;
import com.bangor.empirical.SerialTest;
import com.bangor.utils.UtilityHadoop;
import com.bangor.utils.UtilityMath;
import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.util.MathUtils;
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
            int iDecimalPlaces = Integer.parseInt(args[2]);
            String sEvaluation = args[3];
            double dSignificance = Double.parseDouble(args[4]);
            String sInput = args[5];
            String sOutput = args[6];

            boolean bTestPasses = frequencyTestHandler(iRange, iDecimalPlaces, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);

        } else if (test.equalsIgnoreCase("-S")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iLengthOfPattern = Integer.parseInt(args[1]);
            int iRange = Integer.parseInt(args[2]);
            int iDecimalPlaces = Integer.parseInt(args[3]);
            String sEevaluation = args[4];
            double dSignificance = Double.parseDouble(args[5]);
            String sInput = args[6];
            String sOutput = args[7];

            boolean bTestPasses = serialTestHandler(iLengthOfPattern, iRange, iDecimalPlaces, dSignificance, sEevaluation, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-R")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int iDecimalPlaces = Integer.parseInt(args[2]);
            String sEevaluation = args[3];
            double dSignificance = Double.parseDouble(args[4]);
            String sInput = args[5];
            String sOutput = args[6];

            boolean bTestPasses = runTestHandler(iRange, iDecimalPlaces, dSignificance, sEevaluation, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-G")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int iDecimalPlaces = Integer.parseInt(args[2]);
            float dMinimumLimit = Float.parseFloat(args[3]);
            float dMaximumLimit = Float.parseFloat(args[4]);
            String sEevaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = gapTestHandler(iRange, iDecimalPlaces, dMinimumLimit, dMaximumLimit, sEevaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-P")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int iDecimalPlaces = Integer.parseInt(args[2]);
            int iGroupSize = Integer.parseInt(args[3]);
            String sEevaluation = args[4];
            double dSignificance = Double.parseDouble(args[5]);
            String sInput = args[6];
            String sOutput = args[7];

            boolean bTestPasses = pokerTestHandler(iRange, iDecimalPlaces, iGroupSize, sEevaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-C")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iRange = Integer.parseInt(args[1]);
            int iLength = Integer.parseInt(args[2]);
            int iDecimalPlaces = Integer.parseInt(args[3]);
            float fMinimumValue = Float.parseFloat(args[4]);
            float fMaximumValue = Float.parseFloat(args[5]);
            String sEevaluation = args[6];
            double dSignificance = Double.parseDouble(args[7]);
            String sInput = args[8];
            String sOutput = args[9];

            boolean bTestPasses = couponTestHandler(iRange, iLength, iDecimalPlaces, fMinimumValue, fMaximumValue, sEevaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        }
    }

    /**
     * Handler for Frequency Test
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean frequencyTestHandler(int iRange, int iDecimalPlaces, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        FrequencyTest fTest = new FrequencyTest();
        Job conf = fTest.test(sInput, sOutput);

        String sLocalOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange, 1, true, true).intValue();
//        System.out.println("numOfCombinations = " + numOfCombinations);
        double[] dArrExpected = new double[numOfCombinations];
        for (int i = 0; i < dArrExpected.length; i++) {
            dArrExpected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, sLocalOutput, dArrExpected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean serialTestHandler(int iLengthOfPattern, int iRange, int iDecimalPlaces, double dSignificance, String sEvaluation, String sInput, String sOutput) throws Exception {

        SerialTest sTest = new SerialTest();
        Job conf = sTest.test(iLengthOfPattern, sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);
//        System.out.println("***\t\tiRange = " + iRange);
//        System.out.println("***\t\tiLengthOfPattern = " + iLengthOfPattern);
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange, iLengthOfPattern, true, true).intValue();
        double[] expected = new double[numOfCombinations];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean runTestHandler(int iRange, int iDecimalPlaces, double dSignificance, String sEvaluation, String sInput, String sOutput) throws Exception {

        RunsTest gTest = new RunsTest();
        Job conf = gTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iRange];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean gapTestHandler(int iRange, int iDecimalPlaces, float fMinimumLimit, float fMaximumLimit, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        GapTest gTest = new GapTest(fMinimumLimit, fMaximumLimit);
        Job conf = gTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iRange];
        expected[0] = (fMaximumLimit - fMinimumLimit) / iRange;
        //TODO: Check this with Ryan
        for (int i = 1; i < expected.length; i++) {
            System.out.println("***");
            double first = expected[0] * (1 - expected[0]);
            expected[i] = Math.pow(first, i);
            System.out.println("\tfirst = " + first);
            System.out.println("\texpected[i] = " + expected[i]);
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Poker Test
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean pokerTestHandler(int iRange, int iDecimalPlaces, int iGroupSize, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {

        PokerTest pTest = new PokerTest(iGroupSize);
        Job conf = pTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iGroupSize];
        for (int i = 0; i < expected.length; i++) {
            double dFactLim = UtilityMath.factorialLimit(iRange, iRange - (i + 1) + 1).doubleValue();

            double dPow = Math.pow(iRange, iGroupSize);

            double dDiv = dFactLim / dPow;

            double dSterlingVal = UtilityMath.SterlingNumber(iGroupSize, i + 1).doubleValue();

            expected[i] = dDiv * dSterlingVal;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Coupon Test. This test is non-overlapping
     *
     * @param iRange Range of generator
     * @param iDecimalPlaces the degree to which a number can go
     * @param sEvaluation the evaluation method to be used
     * @param dSignificance the significance level to use in evaluation
     * @param sInput the hadoop input file
     * @param sOutput the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean couponTestHandler(int iRange, int iLength, int iDecimalPlaces, float fMinimumLimit, float fMaximumLimit, String sEvaluation, double dSignificance, String sInput, String sOutput) throws Exception {
        
        System.out.println("fMinimumLimit = " + fMinimumLimit);
        System.out.println("fMaximumLimit = " + fMaximumLimit);
        CouponTest cTest = new CouponTest(fMinimumLimit, fMaximumLimit, iDecimalPlaces);
        Job conf = cTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput + File.separator + sFileName, conf);

        double[] expected = new double[iLength];
        int iRange1 = (int)((fMaximumLimit * iDecimalPlaces+1) - (fMinimumLimit * iDecimalPlaces+1));
        for (int i = 0; i < expected.length; i++) {
            //writing code for pr, not sure if pt comes into this version. see Knuths the art of computer programming
            //this is calculated under the assumption that the minimum value is 0, therefore the range is necessary to include negateives
            long lRangeFactorial = UtilityMath.factorial(iRange1).longValue();
            double dRangePowerI = Math.pow(iRange1, i+1);
            double dStirlingNum = UtilityMath.SterlingNumber(i/*r-1*/, iRange1-1).doubleValue();
            
            
            double dProbI = (lRangeFactorial / dRangePowerI) * dStirlingNum;
            
            expected[i] = dProbI;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected, dSignificance, iRange, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }
}
