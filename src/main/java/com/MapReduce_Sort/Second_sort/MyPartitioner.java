package com.MapReduce_Sort.Second_sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Sort.Second_sort
 * @filename:MyPartitioner.java
 * @create:2019.10.08.19:53:30
 * @auther:李煌民
 * @description:.自定义分区比较器
 **/
public class MyPartitioner extends Partitioner<YearSid, DoubleWritable> {
    @Override
    public int getPartition(YearSid yearSid, //复合键，即map端输出的k2
                            DoubleWritable doubleWritable,//map端输出v2
                            int i//表示reduce的个数，总的分区数
    ) {
        IntWritable year = yearSid.getYear();
        //返回值就是reduce的编号
        return year.hashCode()%i;
    }
}
