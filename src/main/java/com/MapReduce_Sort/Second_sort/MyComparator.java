package com.MapReduce_Sort.Second_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Sort.Second_sort
 * @filename:MyComparator.java
 * @create:2019.10.08.20:04:59
 * @auther:李煌民
 * @description:.自定义分组比较器
 **/
public class MyComparator extends WritableComparator {
    //必须在构造器中指明要比较的数据
    public MyComparator(){
        super(YearSid.class);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearSid ysa = (YearSid) a;
        YearSid ysb = (YearSid) b;
        return ysa.getYear().compareTo(ysb.getYear());
    }
}
