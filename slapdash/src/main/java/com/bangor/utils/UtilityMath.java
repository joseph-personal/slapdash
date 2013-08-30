package com.bangor.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * a class to contain utility Math methods that do not need an isntance to carry
 * out
 *
 * @author Joseph W Plant
 */
public class UtilityMath {

    /**
     * get the amount of possible combinations
     *
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are
     * selecting
     * @param bRepetition whether repetition is enabled (are 1:1 and 2:2
     * allowed?)
     * @param bOrder whether order is important or not (is 1:2 the same as 2:1?)
     * @return BigInteger specifying the amount of possible combinations
     */
    public static BigInteger getCombinationAmount(int iPossibleObservations, int iPatternLength, boolean bRepetition, boolean bOrder) {
        if (bRepetition) {
            if (bOrder) {
//                System.out.println("***\t\tNumOfCombinations: " + UtilityMath.getCombinationAmountWithRepitionWithOrder(iPossibleObservations, iPatternLength));
                return UtilityMath.getCombinationAmountWithRepitionWithOrder(iPossibleObservations, iPatternLength);
            } else {
                return UtilityMath.getCombinationAmountWithRepitionNoOrder(iPossibleObservations, iPatternLength);
            }
        } else {
            if (bOrder) {
                return UtilityMath.getCombinationAmountNoRepWithOrder(iPossibleObservations, iPatternLength);
            } else {
                return UtilityMath.getCombinationAmountNoRepNoOrder(iPossibleObservations, iPatternLength);
            }
        }
    }

    /**
     * get combinations amount where repetition(1:1) is allowed; and Order does
     * not matter (both 1:2 and 2:1 are the same)
     *
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are
     * selecting
     * @return
     */
    public static BigInteger getCombinationAmountWithRepitionNoOrder(int iPossibleObservations, int iPatternLength) {

        BigInteger biTop = UtilityMath.factorial(iPossibleObservations + iPatternLength - 1);
//        BigInteger biTop = MathUtils.factorial(iPossibleObservations + iPatternLength - 1);
        BigInteger biBottom = UtilityMath.factorial(iPatternLength).multiply(UtilityMath.factorial(iPossibleObservations - 1));

        return biTop.divide(biBottom);
    }

    public static BigInteger getCombinationAmountWithRepitionWithOrder(int iPossibleObservations, int iPatternLength) {
        BigInteger biNumOfCombinations = BigInteger.valueOf(iPossibleObservations).pow(iPatternLength);

        return biNumOfCombinations;
    }

    /**
     * get combinations amount where repetition(1:1) is not allowed; and Order
     * does matter (both 1:2 and 2:1 are not the same)
     *
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are
     * selecting
     * @return
     */
    public static BigInteger getCombinationAmountNoRepWithOrder(int iPossibleObservations, int iPatternLength) {
        BigInteger biNumOfCombinations;

        BigInteger biPossibleObservationsFactorial = factorial(iPossibleObservations);
        BigInteger biBottom = factorial(iPossibleObservations - iPatternLength);

        biNumOfCombinations = biPossibleObservationsFactorial.divide(biBottom);

        return biNumOfCombinations;
    }

    /**
     * get combinations amount where repetition(1:1) is not allowed; and Order
     * does not matter (both 1:2 and 2:1 are the same). Otherwise known as
     * Bionomial coefficient [1] .
     *
     * @param iPossibleObservations number of possible observations.
     * @param iPatternLength length of each pattern (number of outcomes we are
     * selecting. According to [1], if patternLength is 0 or the same as
     * iPossibleObservations, this method will return 1
     * @return the combination amount where repetition is not allowed and order
     * is not taken into consideration.
     * @see [1]http://en.wikipedia.org/wiki/Binomial_coefficient
     */
    public static BigInteger getCombinationAmountNoRepNoOrder(int iPossibleObservations, int iPatternLength) {
        if (iPatternLength == 0 || iPatternLength == iPossibleObservations) {
            return BigInteger.ONE;
        }

        BigInteger biNumOfCombinations;

        BigInteger biPossibleObservationsFactorial = factorial(iPossibleObservations);
        BigInteger biPatternLengthFactorial = factorial(iPatternLength);

        BigInteger biLastFactorial = factorial(iPossibleObservations - iPatternLength);

        biNumOfCombinations = biPossibleObservationsFactorial.divide(biPatternLengthFactorial.multiply(biLastFactorial));

        return biNumOfCombinations;
    }

    /**
     * Calculate Factorial of iNumToProcess
     *
     * @param iNumToProcess iNumToPocess! (will calculate the factorial for
     * iNumToProcess)
     * @return the Factorial of iNumToProcess
     */
    public static BigInteger factorial(int iNumToProcess) {
        BigInteger biEndFactorial = BigInteger.valueOf(iNumToProcess);
        for (int i = iNumToProcess - 1; i > 0; i--) {
//            System.out.print("\t" + biEndFactorial + " * " + i + " = ");
            biEndFactorial = biEndFactorial.multiply(BigInteger.valueOf(i));
//            System.out.print(biEndFactorial + "\n");
        }

        return biEndFactorial;
    }

    /**
     * Calculate Factorial of iNumToProcess untilthe multiplication reaches
     * iLimit
     *
     * @param iNumToProcess iNumToPocess (will calculate the factorial for
     * iNumToProcess)
     * @param iLimit stop multiplying when multiplication reaches this limit
     * @return the Factorial of iNumToProcess
     */
    public static BigInteger factorialLimit(int iNumToProcess, int iLimit) {
        BigInteger biEndFactorial = BigInteger.valueOf(iNumToProcess);
        for (int i = iNumToProcess - 1; i >= iLimit; i--) {
//            System.out.print("\t" + biEndFactorial + " * " + i + " = ");
            biEndFactorial = biEndFactorial.multiply(BigInteger.valueOf(i));
//            System.out.print(biEndFactorial + "\n");
        }
        return biEndFactorial;
    }

    /**
     * calculates the sterling number of {n k}
     *
     * @param n
     * @param k
     * @return
     */
    public static BigDecimal SterlingNumber(int n, int k) {
        //return 1 or 0 for special cases
        if(n == k){
            return BigDecimal.ONE;
        } else if(k == 0){
            return BigDecimal.ZERO;
        }
        //calculate first coefficient
        BigDecimal bdCoefficient = BigDecimal.ONE.divide(new BigDecimal(UtilityMath.factorial(k)), MathContext.DECIMAL64);

        //define summation
        BigInteger summation = BigInteger.ZERO;
        for (int i = 0; i <= k; i++) {
            //combination amount = binomial coefficient
            BigInteger biCombinationAmount = UtilityMath.getCombinationAmount(k, i, false, false);
            //biN = i^n
            BigInteger biN = BigInteger.valueOf(i).pow(n);

            //plus this calculation onto previous calculation. 1/k! * E(-1^(k-j) * (k, j) j^n)
            summation = summation.add(BigInteger.valueOf(-1).pow(k - i).multiply(biCombinationAmount).multiply(biN));
        }

        return bdCoefficient.multiply(new BigDecimal(summation)).setScale(0, RoundingMode.UP);
    }

    public static void main(String[] args) {
            System.out.print("\t" + " ");
        for (int i = 0; i <= 10; i++) {
            System.out.print("\t" + i);
        }
            System.out.print("\n");
        for (int i = 0; i <= 10; i++) {
            System.out.print("\t" + i);
            for (int j = 0; j <= 10; j++) {
                int n = i;
                int k = j;
                if (k > i) {
                    System.out.print("\t0");
                    continue;
                }
                BigDecimal biSterling = UtilityMath.SterlingNumber(n, k);
                System.out.print("\t" + biSterling.toPlainString());
            }
            System.out.print("\n");
        }
    }
}
