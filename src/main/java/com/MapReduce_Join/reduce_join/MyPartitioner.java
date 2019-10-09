package com.MapReduce_Join.reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.reduce_join
 * @filename:MyPartitioner.java
 * @create:2019.10.09.18:20:28
 * @auther:李煌民
 * @description:.自定义分区器
 **/
public class MyPartitioner extends Partitioner<KeyFlag, Text> {
    @Override
    public int getPartition(KeyFlag keyFlag, Text text, int i) {
        return keyFlag.getKey().hashCode() % i;
    }
}
