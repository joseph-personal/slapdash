package com.bangor.evaluation;

import com.bangor.exception.ArrayLengthNotEqualException;
import com.bangor.exception.ParameterNotValidException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.math.MathException;

/**
 *
 * @author Joseph W Plant
 */
public class Evaluator {

    private final long[] longArr_observedCount;
    private final double[] doubleArr_expectedCount;
    private double double_significance;
    private String string_evaluationMethod;

    /**
     * Constructor if observed values are currently in file, not data structure
     *
     * @param observedValuesFilePath - path of file containing observed files
     * @param expectedValues double array containing expected values (in ratio
     * to one another)
     * @param significance
     * @param evaluationMethod
     */
    public Evaluator(String evaluationMethod, String observedValuesFilePath,
            double[] expectedValues, double significance){
        longArr_observedCount = parseCategoryArrayFromFile(observedValuesFilePath, expectedValues.length);
        doubleArr_expectedCount = expectedValues;
        double_significance = significance;
        string_evaluationMethod = evaluationMethod;
    }

    /**
     * Constructor if observed and expected values are already in data structure
     *
     * @param observedValues
     * @param expectedValues
     * @param evluationMethod
     */
    public Evaluator(long[] observedValues,
            double[] expectedValues, String evluationMethod) {
        longArr_observedCount = observedValues;
        doubleArr_expectedCount = expectedValues;
    }
    
    public boolean evaluate() throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException{
        
//        System.out.println("Expected");
//        printDoubleArray(doubleArr_expectedCount);
//        System.out.println("Observed");
//        printLongArray(longArr_observedCount);
        
        if(string_evaluationMethod.equalsIgnoreCase("-Ch")){
            ChiSquare chiSquareTest = new ChiSquare(longArr_observedCount, 
                    doubleArr_expectedCount, double_significance);
            
            return chiSquareTest.evaluateChiSquare();
        }
        
      throw new ParameterNotValidException("Evaluation type supplied is not valid");
    }

    private long[] parseCategoryArrayFromFile(String string_filePath, int int_numOfCategories) {

        long[] doubleArr_observedCategories = new long[int_numOfCategories];
        BufferedReader br = null;

        try {
            String sCurrentLine;
            br = new BufferedReader(new FileReader(string_filePath));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] splitCurrentLine = sCurrentLine.split("\\t");
                //TODO: allow catnum to calculate catNum bsaed on double cat, think this is done
                int catNum = Integer.parseInt(splitCurrentLine[0].trim());
//                int catNum = Integer.parseInt("gfdhfdgdfgd");
                int catAmount = Integer.parseInt(splitCurrentLine[splitCurrentLine.length-1].trim());

                doubleArr_observedCategories[catNum] = catAmount;
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

    public double getDouble_significance() {
        return double_significance;
    }

    public void setDouble_significance(double double_significance) {
        this.double_significance = double_significance;
    }

    public String getString_evaluationMethod() {
        return string_evaluationMethod;
    }

    public void setString_evaluationMethod(String string_evaluationMethod) {
        this.string_evaluationMethod = string_evaluationMethod;
    }

    private void printDoubleArray(double[] arrToPrint){
        for (int i = 0; i < arrToPrint.length; i++) {
            System.out.println("["+i+"] " + arrToPrint[i]);
        }
    }

    private void printLongArray(long[] arrToPrint){
        for (int i = 0; i < arrToPrint.length; i++) {
            System.out.println("["+i+"] " + arrToPrint[i]);
        }
    }
    
}
