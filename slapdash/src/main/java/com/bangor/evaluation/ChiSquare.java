package com.bangor.evaluation;

import com.bangor.exception.ArrayLengthNotEqualException;
import com.bangor.exception.ParameterNotValidException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.MathException;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;

/**
 *
 * @author hduser
 */
public class ChiSquare {

    private final ChiSquareTestImpl chiSquareTest;
    private Double dSignificance;
    private double[] darrExpectedCounts;
    private long[] larrObservedCounts;

    /**
     * created for testing class
     *
     * @param args
     */
    public static void main(String[] args) {

        double significance = 0.01;
        double[] expectedCount = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

        long[] observedCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, 50};
//        long[] observedCount = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5};

//        long[] observedCount = new long[10];
//        
//        Random RG = new Random();
//        for (int i = 0; i < 10; i++) {
//            int randInt = RG.nextInt(10);
//            observedCount[randInt] += 1;
//        }
        ChiSquare chiSquareCalc = new ChiSquare(observedCount, expectedCount, significance);

        boolean doesPass = false;
        try {
            doesPass = chiSquareCalc.evaluateChiSquare();
            System.out.println("doesPass = " + doesPass);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(ChiSquare.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MathException ex) {
            Logger.getLogger(ChiSquare.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParameterNotValidException ex) {
            Logger.getLogger(ChiSquare.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ArrayLengthNotEqualException ex) {
            Logger.getLogger(ChiSquare.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("***\nEND OF TEST\n***");
    }

    /**
     * NO CONSTRUCTOR LOGIC
     */
    public ChiSquare() {
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     * SETS THE SIGNIFICANCE OF THE TEST
     *
     * @param dSignificance - the significance (or confidence) of the test
     */
    public ChiSquare(double dSignificance) {
        this.dSignificance = dSignificance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     * SETS THE EXPECTED VALUES AND SIGNIFICANCE OF THE TEST
     *
     * @param darrExpectedCounts - the expected values of the test
     * @param dSignificance - the significance (or confidence) of the test
     */
    public ChiSquare(double[] darrExpectedCounts, double dSignificance) {
        this.darrExpectedCounts = darrExpectedCounts;
        this.dSignificance = dSignificance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     * SETS THE OCCURRED VALUES, EXPECTED VALUES AND SIGNIFICANCE OF THE TEST
     *
     * @param larrObservedCounts - the occurred values of the generator
     * @param darrExpectedCounts - the expected values of the test
     * @param dSignificance - the significance (or confidence) of the test
     */
    public ChiSquare(long[] larrObservedCounts, double[] darrExpectedCounts, 
            double dSignificance) {
        this.larrObservedCounts = larrObservedCounts;
        this.darrExpectedCounts = darrExpectedCounts;
        this.dSignificance = dSignificance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS OF THIS CLASS
     *
     * @return whether the values pass or fail at this significance
     * @throws IllegalArgumentException from ChiSquareTest
     * @throws MathException from ChiSquareTest
     * @throws ParameterNotValidException if no global vars have not been set
     * @throws ArrayLengthNotEqualException if arrays are not equal in length
     */
    public boolean evaluateChiSquare() throws IllegalArgumentException, 
            MathException, ParameterNotValidException, 
            ArrayLengthNotEqualException {
        if (dSignificance != null && darrExpectedCounts != null && 
                larrObservedCounts != null) {
            if (darrExpectedCounts.length == larrObservedCounts.length) {
                return chiSquareTest.chiSquareTest(darrExpectedCounts, 
                        larrObservedCounts, dSignificance);
            } else {
                throw new ArrayLengthNotEqualException(""
                        + "Expected and Observed arrays are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("Global Expected & Observed "
                    + "Arrays and significance value are null. Either use the "
                    + "constructor to set these values or use "
                    + "setDoubleArr_expectedCounts(), "
                    + "setLingArr_observedCounts() & setDouble_significance() "
                    + "first.");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param dSignificance the significance to test with
     * @return whether the values pass or fail at this significance
     * @throws IllegalArgumentException from ChiSquareTest
     * @throws MathException from ChiSquareTest
     * @throws ParameterNotValidException if no expected or occurred global vars
     * have not been set
     * @throws com.bangor.exception.ArrayLengthNotEqualException if Observed and
     * Expected Arrays are not of the same length
     */
    public boolean evaluateChiSquare(double dSignificance) throws IllegalArgumentException,
            MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if ((darrExpectedCounts != null && larrObservedCounts != null)) {
            if (darrExpectedCounts.length == larrObservedCounts.length) {
                return chiSquareTest.chiSquareTest(darrExpectedCounts, larrObservedCounts, dSignificance);
            } else {
                throw new ArrayLengthNotEqualException("Expected and Observed arrays "
                        + "are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("global expected and observed arrays "
                    + "are null. Use setDoubleArr_expectedCounts() and setLongArr_observedCounts() "
                    + "methods first");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param darrExpectedCounts an array of doubles containing the expected
     * values
     * @param dSignificance the significance to test with
     * @return whether the values pass or fail at this significance
     * @throws IllegalArgumentException from ChiSquareTest
     * @throws MathException from ChiSquareTest
     * @throws ParameterNotValidException if no occurred global vars have not
     * been set
     */
    public boolean evaluateChiSquare(double[] darrExpectedCounts, double dSignificance)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if (larrObservedCounts != null) {
            if (darrExpectedCounts.length == larrObservedCounts.length) {
                return chiSquareTest.chiSquareTest(darrExpectedCounts, larrObservedCounts, dSignificance);
            } else {
                throw new ArrayLengthNotEqualException("Expected and Observed Count arrays "
                        + "are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("Global observed array is null, "
                    + "use setLongArr_observedCounts() method first");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param larrObservedCounts an array of longs containing the observed values
     * @param dSignificance the significance to test with
     * @return whether the values pass or fail at this significance
     * @throws IllegalArgumentException from ChiSquareTest
     * @throws MathException from ChiSquareTest
     * @throws ParameterNotValidException if no expected global vars have not
     * been set
     * @throws com.bangor.exception.ArrayLengthNotEqualExceptionif Observed and
     * Expected Arrays are not of the same length
     */
    public boolean evaluateChiSquare(long[] larrObservedCounts, double dSignificance)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if (darrExpectedCounts != null) {
            if (darrExpectedCounts.length == larrObservedCounts.length) {
                return chiSquareTest.chiSquareTest(darrExpectedCounts, larrObservedCounts, dSignificance);
            } else {
                throw new ArrayLengthNotEqualException("Expected and Observed Count arrays "
                        + "are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("Global expected array is null, "
                    + "use setDoubleArr_expectedCounts() first");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param larrObservedCounts - an array of longs containing the observed values
     * @param darrExpectedCounts - an array of doubles containing the expected values
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ArrayLengthNotEqualException - if local expected and observed
     * arrays are not valid
     * @throws ParameterNotValidException - if no expected global vars have not
     * been set
     */
    public boolean evaluateChiSquare(long[] larrObservedCounts, double[] darrExpectedCounts)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {

        if (dSignificance != null) {
            if (darrExpectedCounts.length == larrObservedCounts.length) {
                return chiSquareTest.chiSquareTest(darrExpectedCounts, larrObservedCounts, dSignificance);
            } else {
                throw new ArrayLengthNotEqualException("Expected and Observed Count arrays "
                        + "are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("Global significance value is null, "
                    + "use setDouble_significance() method first");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param larrObservedCounts - an array of longs containing the observed values
     * @param darrExpectedCounts - an array of doubles containing the expected values
     * @param dSignificance - the significance to test with
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ArrayLengthNotEqualException - if local expected and observed
     * arrays are not valid
     */
    public boolean evaluateChiSquare(long[] larrObservedCounts, double[] darrExpectedCounts, double dSignificance)
            throws IllegalArgumentException, MathException, ArrayLengthNotEqualException {
        if (darrExpectedCounts.length == larrObservedCounts.length) {
            return chiSquareTest.chiSquareTest(darrExpectedCounts, larrObservedCounts, dSignificance);
        } else {
            throw new ArrayLengthNotEqualException("Expected and Observed Count arrays "
                    + "are not of equal size");
        }
    }

    /**
     * GETS THE SIGNIFICANCE AMOUNT SPECIFIED
     *
     * @return
     */
    public Double getDouble_significance() {
        return dSignificance;
    }

    /**
     * GETS THE EXPECTED DOUBLE ARRAY SPECIFIED
     *
     * @return
     */
    public double[] getDoubleArr_expectedCounts() {
        return darrExpectedCounts;
    }

    /**
     * GETS THE OBSERVED LONG ARRAY SPECIFIED
     *
     * @return
     */
    public long[] getLongArr_observedCounts() {
        return larrObservedCounts;
    }

    /**
     * SETS THE SIGNIFICANCE LEVEL THAT THIS CLASS WILL TEST WITH
     *
     * @param double_significance
     */
    public void setDouble_significance(Double double_significance) {
        this.dSignificance = double_significance;
    }

    /**
     * SETS THE ARRAY OF EXPECTED DOUBLES THAT THIS CLASS WILL TEST AGAINST
     *
     * @param doubleArr_expected
     */
    public void setDoubleArr_expectedCounts(double[] doubleArr_expected) {
        this.darrExpectedCounts = doubleArr_expected;
    }

    /**
     * SETS THE ARRAY OF OBSERVED LONGS THAT THIS CLASS WILL TEST WITH
     *
     * @param longArr_observed
     */
    public void setLongArr_observedCounts(long[] longArr_observed) {
        this.larrObservedCounts = longArr_observed;
    }
}
