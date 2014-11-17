package com.bangor.empirical;

import com.bangor.evaluation.Evaluator;
import com.bangor.exception.ArrayLengthNotEqualException;
import com.bangor.exception.HadoopJobFailedException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.bangor.exception.ParameterNotValidException;
import com.bangor.utils.UtilityFiles;
import com.bangor.utils.UtilityHadoop;
import com.bangor.utils.UtilityMath;
import org.apache.commons.math.MathException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.kohsuke.args4j.Argument;

/**
 * This class is to allow the creation and execution of a Hadoop MapReduce
 * frequency Test
 *
 * @author Joseph W Plant
 */
public class FrequencyTest extends EmpiricalTest {

    @Argument(metaVar = "int decimal places", index = 0, required = true, usage = "specifies the number of decimal places this data goes up to")
    int iDecimalPlaces;
    @Argument(metaVar = "float minimum value", index = 1, required = true, usage = "specifies the minimum possible value of this data")
    float fMinimumValue;
    @Argument(metaVar = "float maximum value", index = 2, required = true, usage = "specifies the maximum possible value of this data")
    float fMaximumValue;
    @Argument(metaVar = "String evaluation type", index = 3, required = true, usage = "specifies the evaluation type of this test")
    String sEvaluation;
    @Argument(metaVar = "double significance", index = 4, required = true, usage = "specifies the significance level expected of this test")
    double dSignificance;
    @Argument(metaVar = "String input dir", index = 5, required = true, usage = "specifies the input folder of this test")
    String sInput;
    @Argument(metaVar = "String outpur dir", index = 6, required = true, usage = "specifies the output folder of this test")
    String sOutput;

    /**
     * Linear test for the frequency test
     */
    public void testLinear() throws IOException {
        String[] sInputArr = UtilityFiles.readDirLineByLine(new File(sInput));
        HashMap<Integer, Integer> numsMap = new HashMap<Integer, Integer>();

        for(String item : sInputArr){
            int iItem = Integer.parseInt(item);
            int value = 0;
            if(numsMap.containsKey(iItem)){
                value = numsMap.get(iItem);
            }
            numsMap.put(iItem, value + 1);
        }

        UtilityFiles.writeLineByLine(numsMap, new File(sOutput + File.separator + this.sFileName));
    }

    /**
     * RUNS A THE FREQUENCY-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE.
     * OUTPUT IS SET TO THE OUTPUT FILE
     *
     * @return the job executed
     * @throws Exception
     */
    public Job test() throws Exception {

        Job job = new Job(new Configuration(), "frequencyTest");
        job.setJarByClass(FrequencyTest.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(sInput));
        FileOutputFormat.setOutputPath(job, new Path(sOutput));

        boolean jobSuccessful = job.waitForCompletion(true);

        if (!jobSuccessful) {
            throw new HadoopJobFailedException("FrequencyTest not successful");
        }
        return job;
    }

    @Override
    public void executeTest() {

        int iRange1 = (int) (Math.pow(1, iDecimalPlaces)) * (int) (this.fMaximumValue - this.fMinimumValue) + 1;

        String sLocalOutput = null;
        if(isHadoop) {
            Job conf = null;
            try {
                conf = this.test();
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                sLocalOutput = UtilityHadoop.getFileFromHDFS(sOutput
                        + File.separator + sFileName, conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else{
            try {
                this.testLinear();
            } catch (IOException e) {
                e.printStackTrace();
            }
            sLocalOutput = sOutput + File.separator + this.sFileName;
        }
        int numOfCombinations = UtilityMath.getCombinationAmount(iRange1, 1,
                true, true).intValue();
        double[] dArrExpected = new double[numOfCombinations];
        for (int i = 0; i < dArrExpected.length; i++) {
            dArrExpected[i] = 1.0;
        }
        Evaluator evaluator= null;
        if(sLocalOutput != null) {
            evaluator = new Evaluator(sEvaluation, sLocalOutput,
                    dArrExpected, dSignificance, iRange1, iDecimalPlaces);
        }

        boolean isPass = false;
        if(evaluator != null) {
            try {
                isPass = evaluator.evaluate();
            } catch (MathException e) {
                e.printStackTrace();
            } catch (ParameterNotValidException e) {
                e.printStackTrace();
            } catch (ArrayLengthNotEqualException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Passed: " + isPass);
    }

    /**
     * This is the mapping class for this test
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable iwOne = new IntWritable(1);
        private final Text tWord = new Text();

        /**
         * Maps each number into an initial key/value pair This method will
         * ignore any non-numbers (e.g. letters/grammar)
         *
         * @param lwKey    initial key
         * @param tValue   initial value
         * @param cContext initial context
         * @throws IOException
         */
        @Override
        public void map(LongWritable lwKey, Text tValue, Context cContext)
                throws IOException {

            String line = tValue.toString();
            cContext.getConfiguration().getStrings(line);

            String[] lineArr = line.split("\n");
            for (String string : lineArr) {
                if (string.matches("-?\\d+(\\.\\d+)?")) {
                    tWord.set(string);
                    try {
                        cContext.write(tWord, iwOne);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(FrequencyTest.class.getName()).log(
                                Level.SEVERE, null, ex);
                    }
                }
            }
        }
    }

    /**
     * This is the Reduce class for this test
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text,
            IntWritable> {

        /**
         * Reduce the key,value pair into groups.
         *
         * @param tKey      key to group
         * @param itrValues values of each key
         * @param cContext  context of Job
         * @throws IOException
         */
        @Override
        public void reduce(Text tKey, Iterable<IntWritable> itrValues,
                           Context cContext) throws IOException {
            int iSum = 0;
            for (IntWritable value : itrValues) {
                iSum += value.get();
            }
            try {
                cContext.write(tKey, new IntWritable(iSum));
            } catch (InterruptedException ex) {
                Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE,
                        null, ex);
            }
        }
    }
}
