package com.MapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce
 * @filename:AvgNum.java
 * @create:2019.09.29.18:36:27
 * @auther:李煌民
 * @description:.自定义数据类型，用于表示平均值和产生该平均值的个数,该类必须要实现序列化
 **/
public class AvgNum implements Writable {
    private DoubleWritable avg;
    private LongWritable num;

    public AvgNum() {
        //实例化参数
        avg = new DoubleWritable();
        num = new LongWritable();
    }

    public DoubleWritable getAvg() {
        return avg;
    }

    public void setAvg(DoubleWritable avg) {
        this.avg.set(avg.get());
    }

    public void setAvg(double avg){
        this.avg.set(avg);
    }

    public LongWritable getNum() {
        return num;
    }

    public void setNum(LongWritable num) {
        this.num.set(num.get());
    }

    public void setNum(long num){
        this.num.set(num);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.avg.write(dataOutput);
        this.num.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.avg.readFields(dataInput);
        this.num.readFields(dataInput);
    }

}
