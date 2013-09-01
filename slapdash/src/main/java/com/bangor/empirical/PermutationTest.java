package com.bangor.empirical;

import com.bangor.InputFormats.poker.PokerTestInputFormat;
import com.bangor.utils.UtilityArrays;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class is to allow the creation and execution of a Hadoop MapReduce
 * serial Test
 *
 * @author Joseph W Plant
 */
public class PermutationTest {

    private int iGroupSize;

    public PermutationTest(int iGroupSize) {
        this.iGroupSize = iGroupSize;
    }

    /**
     * This is the Map class for this test
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable iwOne = new IntWritable(1);
        private Text tWord = new Text();

        /**
         * Maps each pattern into an initial key/value pair This method will
         * ignore any non-numbers (e.g. letters/grammar)
         *
         * This algorithm is gathered from Knuth's 'The Art of Computer
         * Programming', permutation test
         *
         * @param lwKey initial key
         * @param tValue initial value
         * @param cContext the context of the job
         * @throws IOException
         */
        @Override
        public void map(LongWritable lwKey, Text tValue, Context cContext) throws IOException {

            String data = tValue.toString();
            System.out.println("data: " + data);

            String[] sarrSplitData = data.split(":");
            int f = 0;

            double dLargestObsv;
            double dCurrentObsv;

            for (int r = sarrSplitData.length - 1; r > 0; r--) {
                int iLargestIndex = r;
                for (int j = 0; j <= r - 1; j++) {
                    dCurrentObsv = Double.parseDouble(sarrSplitData[j]);
                    dLargestObsv = Double.parseDouble(sarrSplitData[iLargestIndex]);
//                    System.out.println("***\t[" + j + "] " + dCurrentObsv + " > [" + iLargestIndex + "]" + dLargestObsv + "?");
                    iLargestIndex = (dCurrentObsv > dLargestObsv) ? j : iLargestIndex;
//                    System.out.println("***\tiLargestIndex = " + iLargestIndex);
                }
                f = f * (r + 1) + iLargestIndex;
//                System.out.println("*** largestNumber = " + sarrSplitData[iLargestIndex]);
                sarrSplitData = (String[]) UtilityArrays.swap(sarrSplitData, r, iLargestIndex);

            }
                UtilityArrays.printArray(sarrSplitData, "*** ");

            tWord.set(((Integer) f).toString());

//            Integer iLengthOfSeq = data.split(":").length;
//            tWord.set(iLengthOfSeq.toString());
            try {
                cContext.write(tWord, iwOne);
            } catch (InterruptedException ex) {
                Logger.getLogger(SerialTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * This is the Reduce class for this test
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Reduce the key,value pair into groups.
         *
         * @param tKey key to group
         * @param itrValues values of each key
         * @param cContext The context of the job
         * @throws IOException
         */
        @Override
        public void reduce(Text tKey, Iterable<IntWritable> itrValues, Context cContext) throws IOException {
            int iSum = 0;
            for (IntWritable value : itrValues) {
                iSum += value.get();
            }
            try {
                //            output.collect(key, new IntWritable(sum));
                cContext.write(tKey, new IntWritable(iSum));
            } catch (InterruptedException ex) {
                Logger.getLogger(SerialTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public int getGroupSize() {
        return iGroupSize;
    }

    public void setGroupSize(int iGroupSize) {
        this.iGroupSize = iGroupSize;
    }

    /**
     * RUNS A SERIAL-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE. OUTPUT
     * IS SET TO THE OUTPUT FILE
     *
     * @param sInput Input file of data.
     * @param sOutput File to output reduce data to
     * @return The job
     * @throws Exception
     */
    public Job test(String sInput, String sOutput) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("iGroupSize", iGroupSize);
        Job job = new Job(conf, "PokerTest");
        job.setJarByClass(PermutationTest.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(PokerTestInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(sInput));
        FileOutputFormat.setOutputPath(job, new Path(sOutput));

//        JobClient.runJob(conf);
        job.waitForCompletion(true);

        return job;
    }
}
