package com.bangor.empirical;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SerialTestOldAPI {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int iPatternLength;

        @Override
        public void configure(JobConf conf) {
            iPatternLength = Integer.parseInt(conf.get("iPatternLength"));
        }

        /**
         * Maps each number into an initial key/value pair This method will
         * ignore any non-numbers (e.g. letters/grammar)
         *
         * @param key - initial key
         * @param value - initial value
         * @param output - map of output
         * @param reporter - reporter
         * @throws IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String data = value.toString();

            String[] dataArr = data.split(" ");
            System.out.println("*******");
            System.out.println("dataArr.length = " + dataArr.length);
            System.out.println("value = " + value.toString());
            for (int i = 0; i < dataArr.length; i++) {
                System.out.println("dataArr[i] = " + dataArr[i]);
                String sThisPattern = "";
                for (int j = 0; j < iPatternLength; j++) {
                    sThisPattern += dataArr[i + j];
                }

                //validity check that it is a number
                if (sThisPattern.matches("-?\\d+(\\.\\d+)?")) {
                    word.set(sThisPattern);
                    output.collect(word, one);
                }

            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Reduce the key,value pair into groups.
         *
         * @param key - key to group
         * @param values - values of each key
         * @param output - output map of Map<Text, Intwriteable>
         * @param reporter - reporter
         * @throws IOException
         */
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    /**
     * RUNS A SERIAL-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE. OUTPUT
     * IS SET TO THE OUTPUT FILE
     *
     * @param iPatternLength
     * @param sInput - Input file of data.
     * @param sOutput - file to output reduce data to
     * @return 
     * @throws Exception
     */
    public JobConf test(Integer iPatternLength, String sInput, String sOutput) throws Exception {
        JobConf conf = new JobConf(SerialTestOldAPI.class);
        conf.setJobName("serialTest");
        conf.set("iPatternLength", iPatternLength.toString());

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(sInput));
        FileOutputFormat.setOutputPath(conf, new Path(sOutput));

        JobClient.runJob(conf);

        return conf;
    }
}
