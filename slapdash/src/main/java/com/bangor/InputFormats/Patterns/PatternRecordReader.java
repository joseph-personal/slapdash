package com.bangor.InputFormats.Patterns;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class is to allow the buffering of extra values at each mapper, while
 * these values are still included in other mappers
 *
 * @author Joseph W Plant
 */
public class PatternRecordReader extends RecordReader<LongWritable, Text> {

    //the amount of extra values to add to each mapper, default set in context.get
    private int iBUFFER_SIZE;
    //the value to split each added value.
    private static final String sDELIMITER = ":";

    //the buffers of values/keys
    private final Queue<String> qValueBuffer = new LinkedList<String>();
    private final Queue<Long> qKeyBuffer = new LinkedList<Long>();

    //the key/value
    private final LongWritable lwKey = new LongWritable();
    private final Text tValue = new Text();

    //standard RecordReader
    private final RecordReader<LongWritable, Text> rr;

    /**
     * Constructor, creates global instance of RecordReader for this class
     *
     * @param rr the variable to which this.rr will be set
     */
    public PatternRecordReader(RecordReader<LongWritable, Text> rr) {
        this.rr = rr;
    }

    /**
     * OVERRIDE - Close the recordReader
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        rr.close();
    }

    /**
     * OVERRIDE - gets the current key
     *
     * @return LongWritable this.lwKey
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lwKey;
    }

    /**
     * OVERRIDE - gets the current Value
     *
     * @return Text this.tValue
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return tValue;
    }

    /**
     * OVERRIDE - gets the current progress of this.rr (RecordReader)
     *
     * @return float progress
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return rr.getProgress();
    }

    /**
     * OVERRIDE - initialise method for recordReader. calls initialisation as
     * well as getting this.iBUFFER_SIZE from job context
     *
     * @param arg0 The inputSplit
     * @param arg1 The TaskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        rr.initialize(arg0, arg1);
        this.iBUFFER_SIZE = arg1.getConfiguration().getInt("iPatternLength", 1);
    }

    /**
     * OVERRIDE - gets the nextKeyValue to be processed
     * @return boolean
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (qValueBuffer.isEmpty()) {
            while (qValueBuffer.size() < iBUFFER_SIZE) {
                if (rr.nextKeyValue()) {
                    qKeyBuffer.add(rr.getCurrentKey().get());
                    qValueBuffer.add(rr.getCurrentValue().toString());
                } else {
                    return false;
                }
            }
        } else {
            if (rr.nextKeyValue()) {
                qKeyBuffer.add(rr.getCurrentKey().get());
                qValueBuffer.add(rr.getCurrentValue().toString());
                qKeyBuffer.remove();
                qValueBuffer.remove();
            } else {
                return false;
            }
        }
        lwKey.set(qKeyBuffer.peek());
        tValue.set(getValue());
        return true;
    }

    /**
     * Gets the current value in the class
     * @return String value.
     */
    private String getValue() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = qValueBuffer.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(sDELIMITER);
            }
        }
        return sb.toString();
    }

}
