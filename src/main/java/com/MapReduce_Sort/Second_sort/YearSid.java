package com.MapReduce_Sort.Second_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Sort.Second_sort
 * @filename:YearSid.java
 * @create:2019.10.08.19:13:17
 * @auther:李煌民
 * @description:.自定义一个复合键
 **/
public class YearSid implements WritableComparable<YearSid> {
    private IntWritable year;
    private Text sid;

    //构造方法
    public YearSid() {
        this.year = new IntWritable();
        this.sid = new Text();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearSid yearSid = (YearSid) o;
        return Objects.equals(year, yearSid.year) &&
                Objects.equals(sid, yearSid.sid);
    }

    @Override
    public int hashCode() {
        //因为字符串的哈希值可能相等或者很接近，那么*127，那么有可能为负数，则取绝对值
        return Math.abs(this.year.hashCode() + this.sid.hashCode()*127);
    }

    //key的比较
    @Override
    public int compareTo(YearSid o) {
        int num1 = this.year.compareTo(o.year);
        int num2 = num1==0?this.sid.compareTo(o.sid):num1;
        return num2;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.sid.write(dataOutput);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.sid.readFields(dataInput);
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year.set(year.get());
    }
    public void setYear(int year) {
        this.year.set(year);
    }

    public Text getSid() {
        return sid;
    }

    public void setSid(Text sid) {
        this.sid.set(sid.toString());
    }
    public void setSid(String sid) {
        this.sid.set(sid);
    }

    @Override
    public String toString() {
        return "YearSid{" +
                "year=" + year +
                ", sid=" + sid +
                '}';
    }
}
