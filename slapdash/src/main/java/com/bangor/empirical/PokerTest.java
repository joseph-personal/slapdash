package com.bangor.empirical;

import com.bangor.InputFormats.poker.PokerTestInputFormat;
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
public class PokerTest {

    private int iGroupSize;

    public PokerTest(int iGroupSize) {
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
         * @param lwKey initial key
         * @param tValue initial value
         * @param cContext the context of the job
         * @throws IOException
         */
        @Override
        public void map(LongWritable lwKey, Text tValue, Context cContext) throws IOException {

            int iGroupSize = cContext.getConfiguration().getInt("iGroupSize", 5);
            String data = tValue.toString();
            System.out.println("data: " + data);

            String[] sarrSplitData = data.split(":");
//            float[] farrCounts = new float[iGroupSize];
            int iNumOfDifferences = 5;
//            boolean bIsDuplicate;
//            for (int i = 0; i < sarrSplitData.length; i++) {
//                bIsDuplicate = true;
//                for (int j = 0; j < farrCounts.length; j++) {
//                    if (farrCounts[j] == Float.parseFloat(sarrSplitData[i])) {
//                        farrCounts[j]++;
//                        iNumOfDifferences--;
//                        break;
//                    }
//                    bIsDuplicate = false;
//                }
//                if (!bIsDuplicate) {
//                    farrCounts[i] = Float.parseFloat(sarrSplitData[i]);
//                }
//            }

            //get number of differences in list. don't check same two twice
            for (int i = 0; i < sarrSplitData.length; i++) {
                for (int j = i + 1; j < sarrSplitData.length; j++) {
                    if (sarrSplitData[i].equals(sarrSplitData[j])) {
                        iNumOfDifferences--;
                    }
                }
            }

            System.out.println("***\tiNumOfDifferences = " + iNumOfDifferences);
            tWord.set(((Integer) iNumOfDifferences).toString());

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
     * @param iPatternLength the length of each pattern
     * @param sInput Input file of data.
     * @param sOutput File to output reduce data to
     * @return The job
     * @throws Exception
     */
    public Job test(String sInput, String sOutput) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("iGroupSize", iGroupSize);
        Job job = new Job(conf, "PokerTest");
        job.setJarByClass(PokerTest.class);

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
