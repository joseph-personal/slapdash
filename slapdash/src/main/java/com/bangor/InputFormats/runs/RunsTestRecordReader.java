package com.bangor.InputFormats.runs;

import java.io.IOException;
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

public class RunsTestRecordReader extends RecordReader<LongWritable, Text> {

    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;

    private double dPrevVal = -1;

    private boolean bCalcRunsUp = true;
    private boolean bBeginningOfRun = false;
    private Text tBeginningOfRun;

    private float fMinimumValue = 0;
    private float fMaximumValue = 10;

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
        tBeginningOfRun = new Text("");
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.bCalcRunsUp = conf.getBoolean("bCalcRunsUp", bCalcRunsUp);
        this.fMinimumValue = conf.getFloat("fMinimumValue", fMinimumValue);
        this.fMaximumValue = conf.getFloat("fMaximumValue", fMaximumValue);
        setPrevValToDefault();
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

        boolean bOnRun = true;
        while (bOnRun) {

//            //if we are at beginning of run -> append first observation to value (postponed from end of last run)
//            if (!tBeginningOfRun.toString().isEmpty()) {
//                //this value is to be skipped to implement chi square
////                value.append(tBeginningOfRun.getBytes(), 0, tBeginningOfRun.getLength());
////                value.append(endline.getBytes(), 0, endline.getLength());
//                //set prev value for run comparisons
//                this.dPrevVal = Double.parseDouble(tBeginningOfRun.toString());
//                //beginning of run text set to blank
//                tBeginningOfRun.set("");
//            }
            if (this.bBeginningOfRun) {
                if (this.bCalcRunsUp) {
                    this.dPrevVal = this.fMinimumValue - 1;
                } else {
                    this.dPrevVal = this.fMinimumValue + 1;
                }
            }
            Text v = new Text();
            if (!(pos < end)) {
                bOnRun = false;
                break;
            }
            while (pos < end) {
                //get size of this line, read it into v
                newSize = in.readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                //if line is blank, skip this iteration
                if (newSize == 0) {
                    break;
                }
                //plus size onto position var
                pos += newSize;

                //get this value as double for comparison
                String sNewValue = new String(v.getBytes());
                double dNewValue = Double.parseDouble(sNewValue);

                //valid for if we were on a run up or not
//                boolean bWasRunUp = this.bRunUp;
                //set bRunUp to whether or not this one is a run
//                this.bRunUp = dNewValue >= this.dPrevVal;
                //if we aren't on the beginning of a run, compare the two runUp booleans
                //end this run if runType changes
//                if (!bBeginningOfRun) {
                System.out.println("***checking:\tdPrevVal = " + dPrevVal);
                System.out.println("***checking:\tdNewValue = " + dNewValue);
//                    if (this.bRunUp != bWasRunUp) {
//                        tBeginningOfRun = v;
//                        bOnRun = false;
//                        bBeginningOfRun = true;
//                        break;
//                    }
                if (this.bCalcRunsUp) {
                    //run ended when next value is less than this one
                    if (dNewValue <= this.dPrevVal) {
//                            System.out.println("***\tdPrevVal = " + dPrevVal);
                        tBeginningOfRun = v;
                        bOnRun = false;
                        setPrevValToDefault();
//                            bBeginningOfRun = true;
                        break;
                    }
                } else {
                    //run ended when next value is greater than this one
                    if (dNewValue >= this.dPrevVal) {
                        tBeginningOfRun = v;
                        bOnRun = false;
                        setPrevValToDefault();
//                            bBeginningOfRun = true;
                        break;
                    }
                }

                System.out.println("***appending:\tdNewValue = " + dNewValue);
                //append this value to value text
                value.append(v.getBytes(), 0, v.getLength());
                value.append(endline.getBytes(), 0, endline.getLength());
                //change previous value for this value
                this.dPrevVal = dNewValue;
//                }
                //at this stage, we are no longer at the beginning of a run
//                bBeginningOfRun = false;

                //if our newSize is smaller than the maximum we break out of the nested while loop
                if (newSize < maxLineLength) {
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

    private void setPrevValToDefault() {
        if (this.bCalcRunsUp) {
            this.dPrevVal = this.fMinimumValue - 1;
        } else {
            this.dPrevVal = this.fMinimumValue + 1;
        }
    }
}
