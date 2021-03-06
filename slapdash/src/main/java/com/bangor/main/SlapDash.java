package com.bangor.main;

import com.bangor.empirical.*;
import com.bangor.evaluation.Evaluator;
import com.bangor.exception.PatternNotDivisibleByGroupSizeException;
import com.bangor.utils.UtilityHadoop;
import com.bangor.utils.UtilityMath;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import java.io.File;

/**
 * Class for command line tool api
 *
 * @author Joseph W Plant
 */
public class SlapDash {

    /*Args4j Annotations*/
    @Argument(handler = SubCommandHandler.class, usage = "The test to be run", metaVar = "Empirical test", required = true)
    @SubCommands({
            @SubCommand(name = "frequency", impl = FrequencyTest.class)
    })
    EmpiricalTest test;

    String sFileName = "part-r-00000";

    public static void main(String[] args) {

        SlapDash slapDash = new SlapDash();
        CmdLineParser parser = new CmdLineParser(slapDash);
        try {
            parser.parseArgument(args);
            slapDash.executeTest();
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }

//        try {
//            SlapDash slapDashInst = new SlapDash();
//            slapDashInst.performTestFromCommandLine(args);
//        } catch (Exception ex) {
//            Logger.getLogger(SlapDash.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    /**
     * executes the test specified by the sub command
     */
    public void executeTest() {
        test.executeTest();
    }

    /**
     * Constructor creates instance and carries out tests based on input
     *
     * @param args -arguments from command line
     * @throws Exception
     */
    public void performTestFromCommandLine(String[] args) throws Exception {
        String test = args[0];
        int iDecimalPlaces = Integer.parseInt(args[1]);
        float fMinimumValue = Float.parseFloat(args[2]);
        float fMaximumValue = Float.parseFloat(args[3]);
        if (test.equalsIgnoreCase("-F")) {

            //GET REST OF PARAMETER ARGUMENTS
            String sEvaluation = args[4];
            double dSignificance = Double.parseDouble(args[5]);
            String sInput = args[6];
            String sOutput = args[7];

//            boolean bTestPasses = performFrequencyTest(iDecimalPlaces, fMinimumValue, fMaximumValue, sEvaluation, dSignificance, sInput, sOutput);
//            System.err.println("Pass: " + bTestPasses);

        } else if (test.equalsIgnoreCase("-S")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iLengthOfPattern = Integer.parseInt(args[4]);
            String sEvaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = performSerialTest(iDecimalPlaces, fMinimumValue, fMaximumValue, iLengthOfPattern, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-R")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iSequenceLength = Integer.parseInt(args[4]);
            boolean bCalcRunsUp = Boolean.parseBoolean(args[5]);
            String sEvaluation = args[6];
            double dSignificance = Double.parseDouble(args[7]);
            String sInput = args[8];
            String sOutput = args[9];

            boolean bTestPasses = performRunsTest(iDecimalPlaces, fMinimumValue, fMaximumValue, iSequenceLength, bCalcRunsUp, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-G")) {

            int iSequenceLength = Integer.parseInt(args[4]);
            float fMinOfGap = Float.parseFloat(args[5]);
            float fMaxOfGap = Float.parseFloat(args[6]);
            //GET REST OF PARAMETER ARGUMENTS
            String sEvaluation = args[7];
            double dSignificance = Double.parseDouble(args[8]);
            String sInput = args[9];
            String sOutput = args[10];

            boolean bTestPasses = performGapsTest(iDecimalPlaces, iSequenceLength, fMinimumValue, fMaximumValue, fMinOfGap, fMaxOfGap, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-P")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iGroupSize = Integer.parseInt(args[4]);
            String sEvaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = performPokerTest(iDecimalPlaces, fMinimumValue, fMaximumValue, iGroupSize, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-C")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iSequenceLength = Integer.parseInt(args[4]);
            String sEvaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = performCouponTest(iDecimalPlaces, fMinimumValue, fMaximumValue, iSequenceLength, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        } else if (test.equalsIgnoreCase("-pm")) {

            //GET REST OF PARAMETER ARGUMENTS
            int iGroupSize = Integer.parseInt(args[4]);
            String sEvaluation = args[5];
            double dSignificance = Double.parseDouble(args[6]);
            String sInput = args[7];
            String sOutput = args[8];

            boolean bTestPasses = performPermutationTest(iDecimalPlaces, fMinimumValue, fMaximumValue, iGroupSize, sEvaluation, dSignificance, sInput, sOutput);
            System.err.println("Pass: " + bTestPasses);
        }
    }

    /**
     * Handler for Frequency Test
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @param isHadoop       whether this test is a hadoop test or not
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performFrequencyTest(int iDecimalPlaces,
                                         float fMinimumLimit, float fMaximumLimit, String sEvaluation,
                                         double dSignificance, String sInput, String sOutput, boolean isHadoop)
            throws Exception {

        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;
        FrequencyTest fTest = new FrequencyTest();

        String sLocalOutput;
        if (isHadoop) {
            Job conf = fTest.test();

            sLocalOutput = UtilityHadoop.getFileFromHDFS(sOutput
                    + File.separator + sFileName, conf);
        } else {
            fTest.testLinear();
            sLocalOutput = sOutput + File.pathSeparator + this.sFileName;
        }
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange1, 1,
                true, true).intValue();
        double[] dArrExpected = new double[numOfCombinations];
        for (int i = 0; i < dArrExpected.length; i++) {
            dArrExpected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, sLocalOutput,
                dArrExpected, dSignificance, iRange1, iDecimalPlaces);

        return evaluator.evaluate();
    }

    /**
     * Handler for Serial Test
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param iGroupSize     the size of each group (or pattern) - doesn't handle if
     *                       overall size is not divisible
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performSerialTest(int iDecimalPlaces, float fMinimumLimit,
                                      float fMaximumLimit, int iGroupSize, String sEvaluation,
                                      double dSignificance, String sInput, String sOutput)
            throws Exception {

        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;

//        if (isGroupSizePossible(iRange1, iGroupSize)) {
        SerialTest sTest = new SerialTest();
        Job conf = sTest.test(iGroupSize, sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                + File.separator + sFileName, conf);
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange1,
                iGroupSize, true, true).intValue();
        double[] expected = new double[numOfCombinations];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput,
                expected, dSignificance, iRange1, iDecimalPlaces + 1);

        return evaluator.evaluate();
//        }

//        return false;
    }

    /**
     * Handler for Runs Test
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param bCalcRunUp     whether we are measuring runs up or down
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performRunsTest(int iDecimalPlaces, float fMinimumLimit,
                                    float fMaximumLimit, int iLengthOfSequence, boolean bCalcRunUp,
                                    String sEvaluation, double dSignificance, String sInput,
                                    String sOutput) throws Exception {

//        int iRange1 = (int) ((fMaximumLimit * (iDecimalPlaces + 1))
//                - (fMinimumLimit * (iDecimalPlaces + 1)));
        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;
        RunsTest rTest = new RunsTest(bCalcRunUp, fMinimumLimit, fMaximumLimit);
        Job conf = rTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                + File.separator + sFileName, conf);

        double[] expected = new double[iLengthOfSequence];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = 1.0;
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected,
                dSignificance, iRange1, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Gaps Test
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performGapsTest(int iDecimalPlaces, int iSequenceLength,
                                    float fMinimumLimit, float fMaximumLimit, float fMinOfGapTest,
                                    float fMaxOfGapTest, String sEvaluation, double dSignificance,
                                    String sInput, String sOutput) throws Exception {

//        int iRange1 = (int) ((fMaximumLimit * (iDecimalPlaces + 1))
//                - (fMinimumLimit * (iDecimalPlaces + 1)));
        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;
        GapTest gTest = new GapTest(fMinOfGapTest, fMaxOfGapTest);
        Job conf = gTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                + File.separator + sFileName, conf);

        double[] expected = new double[iSequenceLength];
        expected[0] = (fMaxOfGapTest - fMinOfGapTest) / iRange1;

        for (int i = 1; i < expected.length; i++) {
            double first = expected[0] * (1 - expected[0]);
            expected[i] = Math.pow(first, i);
//            if(i == 228){
//                System.out.println("first = " + first);
//                System.out.println("expected[228] = " + expected[i]);
//            }
            Double d = new Double(1);

            //TODO: THIS WILL REDUCE ACCURACY OF OVERALL TOOL
            if (expected[i] == 0) {
                expected[i] = Double.MIN_VALUE;
            }
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected,
                dSignificance, iRange1, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Poker Test. This test is non-overlapping
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param iGroupSize     the size of each permutation - doesn't handle if
     *                       overall size is not divisible
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performPokerTest(int iDecimalPlaces, float fMinimumLimit,
                                     float fMaximumLimit, int iGroupSize, String sEvaluation,
                                     double dSignificance, String sInput, String sOutput)
            throws Exception {

//        int iRange1 = (int) ((fMaximumLimit * (iDecimalPlaces + 1)) - 
//                (fMinimumLimit * (iDecimalPlaces + 1)));
        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;

        if (isGroupSizePossible(iRange1, iGroupSize)) {
            PokerTest pTest = new PokerTest(iGroupSize);
            Job conf = pTest.test(sInput, sOutput);

            String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                    + File.separator + sFileName, conf);

            //THIS MUST BE +1 TO ACCOUNT FOR 0
            double[] expected = new double[iGroupSize + 1];
            for (int i = 0; i < expected.length; i++) {
                double dFactLim = UtilityMath.factorialLimit(iRange1,
                        iRange1 - (i + 1) + 1).doubleValue();

                double dPow = Math.pow(iRange1, iGroupSize);

                double dDiv = dFactLim / dPow;

                double dSterlingVal = UtilityMath.SterlingNumber(iGroupSize,
                        i + 1).doubleValue();

                expected[i] = dDiv * dSterlingVal;

                //TODO: THIS WILL REDUCE ACCURACY OF OVERALL TOOL
                if (expected[i] == 0) {
                    expected[i] = Double.MIN_VALUE;
                }
            }
            Evaluator evaluator = new Evaluator(sEvaluation, localOutput,
                    expected, dSignificance, iRange1, iDecimalPlaces + 1);

            return evaluator.evaluate();
        }
        return false;
    }

    /**
     * Handler for Coupon Test. This test is non-overlapping
     *
     * @param iDecimalPlaces  the degree to which a number can go
     * @param iSequenceLength the length of the
     * @param sEvaluation     the evaluation method to be used
     * @param dSignificance   the significance level to use in evaluation
     * @param sInput          the hadoop input file
     * @param sOutput         the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception From test (JobConf)
     */
    private boolean performCouponTest(int iDecimalPlaces, float fMinimumLimit,
                                      float fMaximumLimit, int iSequenceLength, String sEvaluation,
                                      double dSignificance, String sInput, String sOutput)
            throws Exception {
//        int iRange1 = (int) ((fMaximumLimit * (iDecimalPlaces + 1))
//                - (fMinimumLimit * (iDecimalPlaces + 1)));
        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;
        CouponTest cTest = new CouponTest(fMinimumLimit, fMaximumLimit,
                iDecimalPlaces);
        Job conf = cTest.test(sInput, sOutput);

        String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                + File.separator + sFileName, conf);

        double[] expected = new double[iSequenceLength];
        for (int i = 0; i < expected.length; i++) {
            //writing code for pr, not sure if pt comes into this version. 
            //see Knuths the art of computer programming
            //this is calculated under the assumption that the minimum value is
            //0, therefore the range is necessary to include negateives
            long lRangeFactorial = UtilityMath.factorial(iRange1).longValue();
            double dRangePowerI = Math.pow(iRange1, i + 1);
            double dStirlingNum = UtilityMath.SterlingNumber(i/*r-1*/, iRange1
                    - 1).doubleValue();

            double dProbI = (lRangeFactorial / dRangePowerI) * dStirlingNum;

            expected[i] = dProbI;

            //TODO: THIS WILL REDUCE ACCURACY OF OVERALL TOOL
            if (expected[i] == 0) {
                expected[i] = Double.MIN_VALUE;
            }
        }
        Evaluator evaluator = new Evaluator(sEvaluation, localOutput, expected,
                dSignificance, iRange1, iDecimalPlaces + 1);

        return evaluator.evaluate();
    }

    /**
     * Handler for Permutation test. This test is non-overlapping
     *
     * @param iDecimalPlaces the degree to which a number can go
     * @param fMinimumLimit  the minimum value that could have been generated
     *                       (inclusive)
     * @param fMaximumLimit  the maximum value that could have been generated
     *                       (inclusive)
     * @param iGroupSize     the size of each permutation - doesn't handle if
     *                       overall size is not divisible
     * @param sEvaluation    the evaluation method to be used
     * @param dSignificance  the significance level to use in evaluation
     * @param sInput         the hadoop input file
     * @param sOutput        the hadoop output file
     * @return boolean on whether sequence has passed or failed
     * @throws Exception
     */
    private boolean performPermutationTest(int iDecimalPlaces,
                                           float fMinimumLimit, float fMaximumLimit, int iGroupSize,
                                           String sEvaluation, double dSignificance, String sInput,
                                           String sOutput) throws Exception {

        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (fMaximumLimit - fMinimumLimit) + 1;

        if (isGroupSizePossible(iRange1, iGroupSize)) {

            PermutationTest pTest = new PermutationTest(iGroupSize);
            Job conf = pTest.test(sInput, sOutput);

            String localOutput = UtilityHadoop.getFileFromHDFS(sOutput
                    + File.separator + sFileName, conf);

            int iGroupSizeFactorial
                    = UtilityMath.factorial(iGroupSize).intValue();
            double[] expected = new double[iGroupSizeFactorial];
            for (int i = 0; i < expected.length; i++) {
                //each permutation is equally likely
                expected[i] = 1.0 / (double) iGroupSizeFactorial;

                //TODO: THIS WILL REDUCE ACCURACY OF OVERALL TOOL
                if (expected[i] == 0) {
                    expected[i] = Double.MIN_VALUE;
                }
            }
            Evaluator evaluator = new Evaluator(sEvaluation, localOutput,
                    expected, dSignificance, iRange1, iDecimalPlaces + 1);

            return evaluator.evaluate();
        }
        return false;
    }

    private boolean isGroupSizePossible(int iRange, int iGroupSize) throws PatternNotDivisibleByGroupSizeException {

        if (iRange % iGroupSize != 0) {
            throw new PatternNotDivisibleByGroupSizeException();

        }
        return true;
    }
}
