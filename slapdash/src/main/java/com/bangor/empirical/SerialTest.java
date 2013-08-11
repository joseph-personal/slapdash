package com.bangor.stat;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SerialTest {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * Maps each number into an initial key/value pair
         * This method will ignore any non-numbers (e.g. letters/grammar)
         * @param key - initial key
         * @param value - initial value
         * @param output - map of output
         * @param reporter - reporter
         * @throws IOException 
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()) {
//                word.set(tokenizer.nextToken());
//                output.collect(word, one);
//            }

            String[] lineArr = line.split("");
            for (String string : lineArr) {
                if (string.matches("-?\\d+(\\.\\d+)?")) {
                    word.set(string);
                    output.collect(word, one);
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Reduce the key,value pair into groups.
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

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(FrequencyTest.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(FrequencyTest.Map.class);
        conf.setCombinerClass(FrequencyTest.Reduce.class);
        conf.setReducerClass(FrequencyTest.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    /**
     * RUNS A FREQUENCY-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE.
     * OUTPUT IS SET TO THE OUTPUT FILE
     * @param input - Input file of data.
     * @param output - file to output reduce data to
     * @throws Exception 
     */
    public void test(String input, String output) throws Exception {
        JobConf conf = new JobConf(FrequencyTest.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(FrequencyTest.Map.class);
        conf.setCombinerClass(FrequencyTest.Reduce.class);
        conf.setReducerClass(FrequencyTest.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
    }
}