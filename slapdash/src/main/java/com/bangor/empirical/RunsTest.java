package com.bangor.empirical;

import com.bangor.InputFormats.runs.RunsTestInputFormat;
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
 * runs Test
 *
 * @author Joseph W Plant
 */
public class RunsTest {

    private final boolean bCalcRunsUp;
    private final float fMinimumValue;
    private final float fMaximumValue;

    public RunsTest(boolean bCalcRunsUp, float fMinimumValue, float fMaximumValue) {
        this.bCalcRunsUp = bCalcRunsUp;
        this.fMinimumValue = fMinimumValue;
        this.fMaximumValue = fMaximumValue;
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
        public void map(LongWritable lwKey, Text tValue, Context cContext) 
                throws IOException {

            String data = tValue.toString();
            Integer iLengthOfSeq = data.split(":").length;
            tWord.set(iLengthOfSeq.toString());
            if (!tWord.toString().isEmpty()) {
                try {
                    cContext.write(tWord, iwOne);
                } catch (InterruptedException ex) {
                    Logger.getLogger(SerialTest.class.getName()).log(
                            Level.SEVERE, null, ex);
                }
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
        public void reduce(Text tKey, Iterable<IntWritable> itrValues, 
                Context cContext) throws IOException {
            int iSum = 0;
            for (IntWritable value : itrValues) {
                iSum += value.get();
            }
            try {
                cContext.write(tKey, new IntWritable(iSum));
            } catch (InterruptedException ex) {
                Logger.getLogger(SerialTest.class.getName()).log(Level.SEVERE, 
                        null, ex);
            }
        }

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
        conf.setBoolean("bCalcRunsUp", bCalcRunsUp);
        conf.setFloat("fMinimumValue", fMinimumValue);
        conf.setFloat("fMaximumValue", fMaximumValue);
        Job job = new Job(conf, "RunsTest");
        job.setJarByClass(RunsTest.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(RunsTestInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(sInput));
        FileOutputFormat.setOutputPath(job, new Path(sOutput));

        job.waitForCompletion(true);

        return job;
    }
}
