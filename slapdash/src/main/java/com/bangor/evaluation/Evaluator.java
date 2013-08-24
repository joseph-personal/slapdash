package com.bangor.evaluation;

import com.bangor.empirical.FrequencyTest;
import com.bangor.exception.ArrayLengthNotEqualException;
import com.bangor.exception.ParameterNotValidException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.MathException;

/**
 *
 * @author Joseph W Plant
 */
public class Evaluator {

    private final long[] larrObservedCount;
    private final double[] darrExpectedCount;
    private double dSignificance;
    private String sEvaluationMethod;
    private int iDegree;
    private int iRange;

    /**
     * Constructor if observed values are currently in file, not data structure
     *
     * @param sObservedValuesFilePath - path of file containing observed files
     * @param darrExpectedValues double array containing expected values (in
     * ratio to one another)
     * @param dSignificance the significance to which the evaluation will adhere
     * to. Must be 0 < dSignificance < 0.5
     * @param sEvaluationMethod the method by which observed values will be
     * evaluated. Currently supports '-Ch'.
     * @param iRange range of possible options (e.g. 0:50 would be iRange=51)
     * @param iDegree the degree point of the options (e.g. 0.1 would be
     * iDegree=10; 0.11 would be iDegree=100)
     */
    public Evaluator(String sEvaluationMethod, String sObservedValuesFilePath,
            double[] darrExpectedValues, double dSignificance, int iRange, int iDegree) {
        this.darrExpectedCount = darrExpectedValues;
        this.dSignificance = dSignificance;
        this.sEvaluationMethod = sEvaluationMethod;
        this.iRange = iRange;
        this.iDegree = iDegree;
        this.larrObservedCount = parseCategoryArrayFromFile(sObservedValuesFilePath, darrExpectedValues.length);
    }

    /**
     * Constructor if observed and expected values are already in data structure
     *
     * @param larrObservedValues long array of observed values (including
     * categories which do not appear
     * @param darrExpectedValues double array of expected categories (all
     * categories + their liklihood to occur)
     * @param dSignificance
     * @param sEvaluationMethod the method by which observed values will be
     * evaluated. Currently supports '-Ch'.
     * @param iRange range of possible options (e.g. 0:50 would be iRange=51)
     * @param iDegree the degree point of the options (e.g. 0.1 would be
     * iDegree=10; 0.11 would be iDegree=100)
     */
    public Evaluator(long[] larrObservedValues,
            double[] darrExpectedValues, double dSignificance, String sEvaluationMethod, int iRange, int iDegree) {
        this.larrObservedCount = larrObservedValues;
        this.darrExpectedCount = darrExpectedValues;
        this.dSignificance = dSignificance;
        this.sEvaluationMethod = sEvaluationMethod;
        this.iRange = iRange;
        this.iDegree = iDegree;
    }

    /**
     * Carries out an evaluation based on this.sEvaluationMethod
     *
     * @return boolean stating whether or not these categories pass
     * @throws IllegalArgumentException from chiSquareTest
     * @throws MathException from chiSquareTest
     * @throws ArrayLengthNotEqualException from chiSquareTest
     * @throws ParameterNotValidException thrown if this.sEvaluationMethod is
     * now supported
     */
    public boolean evaluate() throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {

//        System.out.println("Expected");
//        printDoubleArray(doubleArr_expectedCount);
//        System.out.println("Observed");
//        printLongArray(longArr_observedCount);
        if (sEvaluationMethod.equalsIgnoreCase("-Ch")) {
            ChiSquare chiSquareTest = new ChiSquare(larrObservedCount,
                    darrExpectedCount, dSignificance);

            return chiSquareTest.evaluateChiSquare();
        }

        throw new ParameterNotValidException("Evaluation method supplied is not supported");
    }

    /**
     * Written to parse the Hadoop output file into a long[]
     *
     * @param string_filePath filepath of output array
     * @param int_numOfCategories number of possible catagories.
     * @return a long array holding all catagories and the amount of times they
     * occur. Including categories which did not occur at all
     */
    private long[] parseCategoryArrayFromFile(String string_filePath, int int_numOfCategories) {

        long[] doubleArr_observedCategories = new long[int_numOfCategories];
        BufferedReader br = null;

        try {
            String sCurrentLine;
            br = new BufferedReader(new FileReader(string_filePath));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] splitCurrentLine = sCurrentLine.split("\\t");

                int catNum = getCategoryIndex(splitCurrentLine[0].trim(), this.iRange, this.iDegree);
                System.out.println("***\t\tcatNum = " + catNum);
//                int catNum = Integer.parseInt("gfdhfdgdfgd");
                int catAmount = Integer.parseInt(splitCurrentLine[splitCurrentLine.length - 1].trim());

                try {
                    doubleArr_observedCategories[catNum] = catAmount;
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("***\t\tArrayIndexOutOfBoundsException : " + doubleArr_observedCategories.length + " : " + catNum);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return doubleArr_observedCategories;
    }

    /**
     * Prints out the specified double array
     *
     * @param darrToPrint double array to print
     */
    private void printDoubleArray(double[] darrToPrint) {
        for (int i = 0; i < darrToPrint.length; i++) {
            System.out.println("[" + i + "] " + darrToPrint[i]);
        }
    }

    /**
     * Prints out the specified long array
     *
     * @param larrToPrint long array to print
     */
    private void printLongArray(long[] larrToPrint) {
        for (int i = 0; i < larrToPrint.length; i++) {
            System.out.println("[" + i + "] " + larrToPrint[i]);
        }
    }

    /**
     * Calculates the category index for this particular pattern
     *
     * @param pattern pattern to calculate
     * @param iRange range of possible options (e.g. 0:50 would be iRange=51)
     * @param iDegree the degree point of the options (e.g. 0.1 would be
     * iDegree=10; 0.11 would be iDegree=100)
     * @return returns the category index for this particular pattern
     */
    private int getCategoryIndex(String pattern, int iRange, int iDegree) {
        //TODO: this won't work if range does not start from 0
        //TODO: make this check if this amount of patterns is possible. (e.g. does iRange divide by pattern amount with no remainders)
        int iCategoryIndex = 0;

        if (pattern.contains(":")) {
            //colon is delimiter
            String[] sarrSplitPatternColon = pattern.split(":");
            
            for(int i = sarrSplitPatternColon.length-1; i > -1; i--){
                int thisObs = (int) (Double.parseDouble(sarrSplitPatternColon[i]) * (double) (iDegree));
                int parentNum = 1;
                if(i != 0){
                    parentNum = (int) Math.pow(iRange, i);
                }
                iCategoryIndex += thisObs * (parentNum);
            }
//            //colon is delimiter
//            String[] sarrSplitPatternColon = pattern.split(":");
//            int firstHalf = (int) (Double.parseDouble(sarrSplitPatternColon[0]) * (double) (iDegree));
//            int secondHalf = (int) (Double.parseDouble(sarrSplitPatternColon[1]) * (double) (iDegree));
//
//            iCategoryIndex = firstHalf * iRange + secondHalf;

        } else {
            iCategoryIndex = (int) (Double.parseDouble(pattern) * (double) iDegree);
        }
        return iCategoryIndex;
    }

    /**
     * get the current value of variable degree
     *
     * @return int iDegree
     */
    public int getDegree() {
        return iDegree;
    }

    /**
     * gets the current value of variable EvaluationMethod (the evaluation
     * method currently being used)
     *
     * @return String sEvaluationMethod
     */
    public String getEvaluationMethod() {
        return sEvaluationMethod;
    }

    /**
     * gets the current value of variable significance (the current significance
     * value being used)
     *
     * @return double dSignificance
     */
    public double getSignificance() {
        return dSignificance;
    }

    /**
     * sets the value of this.iDregree
     *
     * @param iDegree the value to which this.iDegree will be set
     */
    public void setDegree(int iDegree) {
        this.iDegree = iDegree;
    }

    /**
     * sets the value of this.sEvaluationMethod
     *
     * @param string_evaluationMethod the value to which this.sEvaluationMethod
     * will be set
     */
    public void setEvaluationMethod(String string_evaluationMethod) {
        this.sEvaluationMethod = string_evaluationMethod;
    }

    /**
     * sets the value of this.dSignificance
     *
     * @param dsignificance the value to which this.dSignificance will be set
     */
    public void setSignificance(double dsignificance) {
        this.dSignificance = dsignificance;
    }

}
