package com.Hadoop_IO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:StudentWritable.java
 * @create:2019.09.25.16:14:09
 * @auther:李煌民
 * @description:.自定义hadoop数据类型
 *                  1、除了重写Comparable中的compareTo方法
 *                  2、还要重写Object中的equals方法和hashCode方法
 *                  3、equals方法和hashCode方法在重写时只需要根据compareTo方法中用到的字段重写即可
 **/
public class StudentWritable implements WritableComparable<StudentWritable> {
    private IntWritable id;
    private Text name;

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        boolean flag = false;
        if (this == obj){
            flag = true;
        }
        if (obj instanceof StudentWritable){
            StudentWritable sw = (StudentWritable) obj;
            if (this.id.equals(sw.id)){
                flag = true;
            }
        }else {
            flag = false;
        }

        return flag;
    }

    public StudentWritable() {
        this.id = new IntWritable();
        this.name = new Text();
    }

    //创建比较规则的方法
    @Override
    public int compareTo(StudentWritable o) {
        int num1 = this.id.compareTo(o.id);
        int num2 = this.name.compareTo(o.name);
        return num1==0?num2:num1;
    }

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.name.write(dataOutput);
    }

    //非序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //先序列化谁那么先反序列化谁
        this.id.readFields(dataInput);
        this.name.readFields(dataInput);
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }
}
