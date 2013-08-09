package com.bangor.general;

import com.bangor.exception.ArrayLengthNotEqualException;
import com.bangor.exception.IntegerNotValidException;
import com.bangor.exception.ParameterNotValidException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.MathException;
import org.apache.commons.math.stat.inference.ChiSquareTest;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;

/**
 *
 * @author hduser
 */
public class ChiSquare{

    private final ChiSquareTestImpl chiSquareTest;
    private Double double_significance;
    private double[] doubleArr_expectedCounts;
    private long[] longArr_observedCounts;

    /**
     * created for testing class
     *
     * @param args
     */
    public static void main(String[] args) {

        double significance = 0.01;
        double[] expected = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        
//        long[] observed = {0, 0, 0, 0, 0, 0, 0, 0, 0, 50};
        long[] observed = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5};

        ChiSquare chiSquareCalc = new ChiSquare(observed, expected, significance);

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
     * @param significance - the significance (or confidence) of the test
     */
    public ChiSquare(double significance) {
        this.double_significance = significance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     * SETS THE EXPECTED VALUES AND SIGNIFICANCE OF THE TEST
     *
     * @param expectedCounts - the expected values of the test
     * @param significance - the significance (or confidence) of the test
     */
    public ChiSquare(double[] expectedCounts, double significance) {
        this.doubleArr_expectedCounts = expectedCounts;
        this.double_significance = significance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     * SETS THE OCCURRED VALUES, EXPECTED VALUES AND SIGNIFICANCE OF THE TEST
     *
     * @param observedCounts - the occurred values of the generator
     * @param expectedCounts - the expected values of the test
     * @param significance - the significance (or confidence) of the test
     */
    public ChiSquare(long[] observedCounts, double[] expectedCounts, double significance) {
        this.longArr_observedCounts = observedCounts;
        this.doubleArr_expectedCounts = expectedCounts;
        this.double_significance = significance;
        chiSquareTest = new ChiSquareTestImpl();
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS OF THIS CLASS
     *
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ParameterNotValidException - if no global vars have not been set
     */
    public boolean evaluateChiSquare() throws IllegalArgumentException, MathException,
            ParameterNotValidException, ArrayLengthNotEqualException {
        if (double_significance != null && doubleArr_expectedCounts != null && longArr_observedCounts != null) {
            if (doubleArr_expectedCounts.length == longArr_observedCounts.length) {
                return chiSquareTest.chiSquareTest(doubleArr_expectedCounts, longArr_observedCounts, double_significance);
            } else {
                throw new ArrayLengthNotEqualException("Expected and Observed arrays "
                        + "are not of equal size");
            }
        } else {
            throw new ParameterNotValidException("Global Expected & Observed Arrays "
                    + "and significance value are null. Either use the constructor "
                    + "to set these values or use setDoubleArr_expectedCounts(), "
                    + "setLingArr_observedCounts() & setDouble_significance() first.");
        }
    }

    /**
     *
     * EVALUATES CHI SQUARE BASED ON GETS/SETS FOR EXPECTED/OCCURRED VALUES OF
     * THIS CLASS
     *
     * @param significance - the significance to test with
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ParameterNotValidException - if no expected or occurred global
     * vars have not been set
     */
    public boolean evaluateChiSquare(double significance) throws IllegalArgumentException,
            MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if ((doubleArr_expectedCounts != null && longArr_observedCounts != null)) {
            if (doubleArr_expectedCounts.length == longArr_observedCounts.length) {
                return chiSquareTest.chiSquareTest(doubleArr_expectedCounts, longArr_observedCounts, significance);
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
     * @param expectedCounts - an array of doubles containing the expected values
     * @param significance - the significance to test with
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ParameterNotValidException - if no occurred global vars have not
     * been set
     */
    public boolean evaluateChiSquare(double[] expectedCounts, double significance)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if (longArr_observedCounts != null) {
            if (expectedCounts.length == longArr_observedCounts.length) {
                return chiSquareTest.chiSquareTest(expectedCounts, longArr_observedCounts, significance);
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
     * @param observed - an array of longs containing the observed values
     * @param significance - the significance to test with
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ParameterNotValidException - if no expected global vars have not
     * been set
     */
    public boolean evaluateChiSquare(long[] observedCounts, double significance)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {
        if (doubleArr_expectedCounts != null) {
            if (doubleArr_expectedCounts.length == observedCounts.length) {
                return chiSquareTest.chiSquareTest(doubleArr_expectedCounts, observedCounts, significance);
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
     * @param observed - an array of longs containing the observed values
     * @param expected - an array of doubles containing the expected values
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ArrayLengthNotEqualException - if local expected and observed
     * arrays are not valid
     * @throws ParameterNotValidException - if no expected global vars have not
     * been set
     */
    public boolean evaluateChiSquare(long[] observedCounts, double[] expectedCounts)
            throws IllegalArgumentException, MathException, ParameterNotValidException, ArrayLengthNotEqualException {

        if (double_significance != null) {
            if (expectedCounts.length == observedCounts.length) {
                return chiSquareTest.chiSquareTest(expectedCounts, observedCounts, double_significance);
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
     * @param observed - an array of longs containing the observed values
     * @param expected - an array of doubles containing the expected values
     * @param significance - the significance to test with
     * @return - whether the values pass or fail at this significance
     * @throws IllegalArgumentException - from ChiSquareTest
     * @throws MathException - from ChiSquareTest
     * @throws ArrayLengthNotEqualException - if local expected and observed
     * arrays are not valid
     */
    public boolean evaluateChiSquare(long[] observedCounts, double[] expectedCounts, double significance)
            throws IllegalArgumentException, MathException, ArrayLengthNotEqualException {
        if (expectedCounts.length == observedCounts.length) {
            return chiSquareTest.chiSquareTest(expectedCounts, observedCounts, significance);
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
        return double_significance;
    }

    /**
     * GETS THE EXPECTED DOUBLE ARRAY SPECIFIED
     *
     * @return
     */
    public double[] getDoubleArr_expectedCounts() {
        return doubleArr_expectedCounts;
    }

    /**
     * GETS THE OBSERVED LONG ARRAY SPECIFIED
     *
     * @return
     */
    public long[] getLongArr_observedCounts() {
        return longArr_observedCounts;
    }

    /**
     * SETS THE SIGNIFICANCE LEVEL THAT THIS CLASS WILL TEST WITH
     *
     * @param double_significance
     */
    public void setDouble_significance(Double double_significance) {
        this.double_significance = double_significance;
    }

    /**
     * SETS THE ARRAY OF EXPECTED DOUBLES THAT THIS CLASS WILL TEST AGAINST
     *
     * @param doubleArr_expected
     */
    public void setDoubleArr_expectedCounts(double[] doubleArr_expected) {
        this.doubleArr_expectedCounts = doubleArr_expected;
    }

    /**
     * SETS THE ARRAY OF OBSERVED LONGS THAT THIS CLASS WILL TEST WITH
     *
     * @param longArr_observed
     */
    public void setLongArr_observedCounts(long[] longArr_observed) {
        this.longArr_observedCounts = longArr_observed;
    }
}
