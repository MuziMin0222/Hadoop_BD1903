package com.MapReduce_Join.reduce_join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.reduce_join
 * @filename:MyComparator.java
 * @create:2019.10.09.18:34:28
 * @auther:李煌民
 * @description:.自定义分组比较器
 **/
public class MyComparator extends WritableComparator {
    public MyComparator(){
        super(KeyFlag.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyFlag kfa = (KeyFlag) a;
        KeyFlag kfb = (KeyFlag) b;
        return kfa.getKey().compareTo(kfb.getKey());
    }
}
