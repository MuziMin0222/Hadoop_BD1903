package com.MapReduce_Sort.Total_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Sort
 * @filename:YearSid.java
 * @create:2019.09.30.09:36:16
 * @auther:李煌民
 * @description:.自定义复合键
 **/
public class YearSid implements WritableComparable<YearSid> {
    private IntWritable year;
    private Text sid;

    public YearSid() {
        year = new IntWritable();
        sid = new Text();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof YearSid)) return false;

        YearSid that = (YearSid)o;
        return this.year.equals(that.year) && this.sid.equals(that.sid);
    }

    @Override
    public int hashCode() {
        return Math.abs(this.year.hashCode() + this.sid.hashCode() * 127);
    }

    @Override
    public int compareTo(YearSid o) {
        int yearcomp = this.year.compareTo(o.year);
        return yearcomp==0?this.sid.compareTo(o.sid):yearcomp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.sid.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.sid.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "YearSid{" +
                "year=" + this.year.get() +
                ", sid=" +this.sid.toString() +
                '}';
    }

    public int getYear() {
        return year.get();
    }

    public void setYear(int year) {
        this.year.set(year);
    }

    public String getSid() {
        return sid.toString();
    }

    public void setSid(String sid) {
        this.sid.set(sid);
    }
}
