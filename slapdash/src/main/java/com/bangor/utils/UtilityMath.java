package com.bangor.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * a class to contain utility Math methods that do not need an isntance to carry out
 * @author Joseph W Plant
 */
public class UtilityMath {
    
    /**
     * get the amount of possible combinations
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are selecting
     * @param bRepetition whether repetition is enabled (are 1:1 and 2:2 allowed?)
     * @param bOrder whether order is important or not (is 1:2 the same as 2:1?)
     * @return BigInteger specifying the amount of possible combinations
     */
    public static BigInteger getCombinationAmount(int iPossibleObservations, int iPatternLength, boolean bRepetition, boolean bOrder){
        if(bRepetition){
            if(bOrder){
                System.out.println("***\t\tNumOfCombinations: " + UtilityMath.getCombinationAmountWithRepitionWithOrder(iPossibleObservations, iPatternLength));
                return UtilityMath.getCombinationAmountWithRepitionWithOrder(iPossibleObservations, iPatternLength);
            } else{
                return UtilityMath.getCombinationAmountWithRepitionNoOrder(iPossibleObservations, iPatternLength);
            }
        } else{
            if(bOrder){
                return UtilityMath.getCombinationAmountNoRepWithOrder(iPossibleObservations, iPatternLength);
            } else{
                UtilityMath.getCombinationAmountNoRepNoOrder(iPossibleObservations, iPatternLength);
            }
        }
        
        return null;
    }

    /**
     * get combinations amount where repetition(1:1) is allowed; and Order does not matter (both 1:2 and 2:1 are the same)
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are selecting
     * @return 
     */
    public static BigInteger getCombinationAmountWithRepitionNoOrder(int iPossibleObservations, int iPatternLength) {
        
        BigInteger biTop = UtilityMath.factorial(iPossibleObservations + iPatternLength - 1);
        BigInteger biBottom = UtilityMath.factorial(iPatternLength).multiply(UtilityMath.factorial(iPossibleObservations -1));

        return biTop.divide(biBottom);
    }

    public static BigInteger getCombinationAmountWithRepitionWithOrder(int iPossibleObservations, int iPatternLength) {
        BigInteger biNumOfCombinations = BigInteger.valueOf(iPossibleObservations).pow(iPatternLength);
        
        return biNumOfCombinations;
    }

    /**
     * get combinations amount where repetition(1:1) is not allowed; and Order does matter (both 1:2 and 2:1 are not the same)
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are selecting
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
     * get combinations amount where repetition(1:1) is not allowed; and Order does not matter (both 1:2 and 2:1 are the same)
     * @param iPossibleObservations number of possible observations
     * @param iPatternLength length of each pattern (number of outcomes we are selecting
     * @return 
     */
    public static BigInteger getCombinationAmountNoRepNoOrder(int iPossibleObservations, int iPatternLength) {
        BigInteger biNumOfCombinations;

        BigInteger biPossibleObservationsFactorial = factorial(iPossibleObservations);
        BigInteger biPatternLengthFactorial = factorial(iPatternLength);

        BigInteger biLastFactorial = factorial(iPossibleObservations - iPatternLength);

        biNumOfCombinations = biPossibleObservationsFactorial.divide(biPatternLengthFactorial.multiply(biLastFactorial));

        return biNumOfCombinations;
    }

    /**
     * Calculate Factorial of iNumToProcess
     * @param iNumToProcess iNumToPocess! (will calculate the factorial for iNumToProcess)
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

    
}
