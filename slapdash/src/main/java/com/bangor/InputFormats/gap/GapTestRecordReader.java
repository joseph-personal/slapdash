package com.bangor.InputFormats.gap;

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

public class GapTestRecordReader extends RecordReader<LongWritable, Text> {

    private final int NLINESTOPROCESS = 3;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;

    private double dPrevVal = -1;

    private boolean bRunUp = true;
    private boolean bBeginningOfRun = false;
    private Text tBeginningOfRun;

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
//        for(int i=0;i<NLINESTOPROCESS;i++){
//            Text v = new Text();
//            while (pos < end) {
//                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
//                
//                
//                value.append(v.getBytes(),0, v.getLength());
//                value.append(endline.getBytes(),0, endline.getLength());
//                if (newSize == 0) {
//                    break;
//                }
//                pos += newSize;
//                if (newSize < maxLineLength) {
//                    break;
//                }
//            }
//        }

        boolean bOnRun = true;
        while (bOnRun) {

            if (!tBeginningOfRun.toString().isEmpty()) {
                System.out.println("Beginning of new run!");
                value.append(tBeginningOfRun.getBytes(), 0, tBeginningOfRun.getLength());
                value.append(endline.getBytes(), 0, endline.getLength());
                this.dPrevVal = Double.parseDouble(tBeginningOfRun.toString());
                tBeginningOfRun.set("");
//                bBeginningOfRun = false;
            }

            Text v = new Text();
            if (!(pos < end)) {
                bOnRun = false;
                break;
            }
            while (pos < end) {
                System.out.println("in pos loop\n");
                newSize = in.readLine(v, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                System.out.println("newSize = " + newSize);
                String sNewValue = new String(v.getBytes());
                double dNewValue = Double.parseDouble(sNewValue);
                System.out.println("dNewValue = " + dNewValue);
                System.out.println("dPrevVal = " + dPrevVal + "\n");

                boolean bWasRunUp = this.bRunUp;

                this.bRunUp = dNewValue >= this.dPrevVal;
                System.out.println("dPrevVal = " + dPrevVal);
                System.out.println("bRunUp = " + bRunUp + "\n");

                System.out.println("bBeginningOfRun = " + bBeginningOfRun + "\n");
                if (!bBeginningOfRun) {
                    if (this.bRunUp != bWasRunUp) {
                        tBeginningOfRun = v;
                        bOnRun = false;
                        bBeginningOfRun = true;
                        System.out.println("breaking out in run checker");
                        break;
                    }
                }
                bBeginningOfRun = false;

                this.dPrevVal = dNewValue;

                value.append(v.getBytes(), 0, v.getLength());
                value.append(endline.getBytes(), 0, endline.getLength());

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
}
