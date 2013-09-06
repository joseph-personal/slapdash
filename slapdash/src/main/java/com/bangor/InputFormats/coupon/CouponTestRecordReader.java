package com.bangor.InputFormats.coupon;

import com.bangor.InputFormats.poker.*;
import com.bangor.empirical.SerialTest;
import com.bangor.utils.UtilityArrays;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CouponTestRecordReader extends RecordReader<LongWritable, Text> {

    private int NLINESTOPROCESS = 1;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;
    private float fMinimumLimit;
    private float fMaximumLimit;
    private int iDecimalPlace;

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.NLINESTOPROCESS = conf.getInt("iGroupSize", 5);
        this.fMinimumLimit = conf.getFloat("fMinimumLimit", 1);
        this.fMaximumLimit = conf.getFloat("fMaximumLimit", 1);
        this.iDecimalPlace = conf.getInt("iDecimalPlace", 0);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end = start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());

        if (start != 0) {
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein, conf);
        if (skipFirstLine) {
            start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text(":");
        int newSize = 0;

        int iSizeOfArray = (int) (this.fMaximumLimit - (this.fMinimumLimit - 1)) * (this.iDecimalPlace + 1);
        System.out.println("iDecimalPlace = " + iDecimalPlace);
        Float[] darrOccurences = new Float[iSizeOfArray];
        Arrays.fill(darrOccurences, fMinimumLimit - 1);
        int i = 0;
        boolean bOnRun = true;
        while (bOnRun) {

            Text v = new Text();
            if (pos >= end) {
                bOnRun = false;
                break;
            }
            while (pos < end) {
                newSize = in.readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                String sNewValue = new String(v.getBytes());
                float fNewValue = Float.parseFloat(sNewValue);

                value.append(v.getBytes(), 0, v.getLength());
                value.append(endline.getBytes(), 0, endline.getLength());

                //add this value to array and segment if it isn't in array yet
                if (!UtilityArrays.contains(darrOccurences, fNewValue)) {
                    try {
                        System.out.println("Putting value in array: " + fNewValue);
                        
                        darrOccurences[i] = fNewValue;
                        //end of this segment if darrOccurences no longer contains dMinNumber-1
                        if (UtilityArrays.contains(darrOccurences, fMinimumLimit - 1)) {
                            System.out.println("have whole collection. \n\tlast to process: " + fNewValue);
                            bOnRun = false;
                        }
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        System.err.println("***\t Trying to access: " + i + "\n***\tlength: " + iSizeOfArray);
                        Logger.getLogger(SerialTest.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    i++;
                    break;
                }
                System.out.println("Value already in array: " + fNewValue);

                if (newSize < maxLineLength) {
                    System.out.println("break: newSize < maxLineLength");
                    break;
                }
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}
