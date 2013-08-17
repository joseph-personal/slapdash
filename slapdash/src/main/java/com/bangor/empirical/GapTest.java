package com.bangor.empirical;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//TODO: COMPLETE THIS CLASS
public class GapTest {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private double dLowerLimit;
        private double dUpperLimit;

        @Override
        public void configure(JobConf conf) {
            dLowerLimit = Double.parseDouble(conf.get("dLowerLimit"));
            dUpperLimit = Double.parseDouble(conf.get("dUpperLimit"));
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
            //values as string
            String data = value.toString();

            //TODO: CHANGE THIS FOR ALL TESTS TO ALLOW DECIMAL PLACE NUMBERS
            //split each individual datum into an array
            String[] dataArr = data.split("");

            //loop through dataArray checking for an entry that's between limits
            for (int i = 0; i < dataArr.length; i++) {
                //if it's a digit
                if (dataArr[i].matches("-?\\d+(\\.\\d+)?")) {
                    //convert string to double
                    double dThisRandomNumber = Double.parseDouble(dataArr[i]);
                    
                    //variable to specify the length of this gap
                    Integer iGapLength = 0;

                    //if this random number is between limits
                    if (dThisRandomNumber >= dLowerLimit && dThisRandomNumber <= dUpperLimit) {
                        
                        //boolean for whether this variable is between limits
                        boolean bInRange = false;
                        for (int j = i; !bInRange; j++) {
                            double dRandomNumberToCheck = Double.parseDouble(dataArr[j]);
                            //if the variable we are checking is not in range
                            if ((dRandomNumberToCheck >= dLowerLimit && dRandomNumberToCheck <= dUpperLimit)) {
                                bInRange = true;
                            }
                            iGapLength++;
                        }
                    }
                    word.set(iGapLength.toString());
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
     * RUNS A FREQUENCY-BASED MAP/REDUCE ON THE DATA INSIDE THE INPUT FILE.
     * OUTPUT IS SET TO THE OUTPUT FILE
     *
     * @param sInput - Input file of data.
     * @param sOutput - file to output reduce data to
     * @param dLowerLimit
     * @param dUpperRange
     * @return
     * @throws Exception
     */
    public JobConf test(String sInput, String sOutput, Double dLowerLimit, Double dUpperRange) throws Exception {
        JobConf conf = new JobConf(GapTest.class);
        conf.setJobName("freqTest");
        conf.set("dLowerRange", dLowerLimit.toString());
        conf.set("dHigherRange", dUpperRange.toString());

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
