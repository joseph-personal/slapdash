package com.bangor.empirical;

import com.bangor.exception.HadoopJobFailedException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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

/**
 * This class is to allow the creation and execution of a Hadoop MapReduce
 * frequency Test
 *
 * @author Joseph W Plant
 */
public class FrequencyTest {

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
         * @param lwKey initial key
         * @param tValue initial value
         * @param cContext initial context
         * @throws IOException
         */
        @Override
        public void map(LongWritable lwKey, Text tValue, Context cContext) throws IOException {

            String line = tValue.toString();
            cContext.getConfiguration().getStrings(line);

            String[] lineArr = line.split("\n");
            for (String string : lineArr) {
                if (string.matches("-?\\d+(\\.\\d+)?")) {
                    tWord.set(string);
                    try {
                        //                    output.collect(word, one);
                        cContext.write(tWord, iwOne);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(FrequencyTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
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
         * @param cContext context of Job
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
                Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * RUNS A THE FREQUENCY-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE. OUTPUT
     * IS SET TO THE OUTPUT FILE
     *
     * @param sInput Input file of data.
     * @param sOutput file to output reduce data to
     * @return the job executed
     * @throws Exception
     */
    public Job test(String sInput, String sOutput) throws Exception {

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

//        JobClient.runJob(conf);
        boolean jobSuccessful = job.waitForCompletion(true);

        if (!jobSuccessful) {
            throw new HadoopJobFailedException("FrequencyTest not successful");
        }
        return job;
    }
}
