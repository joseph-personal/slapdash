package com.bangor.InputFormats.serial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * This class is to override
 * 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'to call
 * SerialTestRecordReader instead of the default
 *
 * @author Joseph W Plant
 */
public class SerialTestInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) {

        return new SerialTestRecordReader(super.createRecordReader(arg0, arg1));
    }
}
