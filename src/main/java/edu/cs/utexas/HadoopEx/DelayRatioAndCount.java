package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayRatioAndCount implements Writable, Comparable<DelayRatioAndCount> {

    private Text airline;
    private DoubleWritable delayRatio;

    public DelayRatioAndCount() {
        this.airline = new Text();
        this.delayRatio = new DoubleWritable();
    }

    public DelayRatioAndCount(Text airline, DoubleWritable delayRatio) {
        this.airline = airline;
        this.delayRatio = delayRatio;
    }

    public Text getAirline() {
        return airline;
    }

    public DoubleWritable getDelayRatio() {
        return delayRatio;
    }

    public void setAirline(Text airline) {
        this.airline = airline;
    }

    public void setDelayRatio(DoubleWritable delayRatio) {
        this.delayRatio = delayRatio;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        airline.write(dataOutput);
        delayRatio.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airline.readFields(dataInput);
        delayRatio.readFields(dataInput);
    }

    @Override
    public int compareTo(DelayRatioAndCount other) {
        return this.delayRatio.compareTo(other.delayRatio);
    }

    @Override
    public String toString() {
        return airline + " " + delayRatio;
    }
}