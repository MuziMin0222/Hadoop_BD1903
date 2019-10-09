package com.JobControl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.JobControl
 * @filename:CalcMaxTemp.java
 * @create:2019.10.09.21:42:53
 * @auther:李煌民
 * @description:.计算最高温度
 **/
public class CalcMaxTemp extends Configured {
    static class CalcMaxTempReduce extends Reducer<Text,Text,Text, DoubleWritable> {
        private Text k3 = new Text();
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            double max = -Double.MAX_VALUE;
            for (Text v2 : v2s) {

            }
        }
    }
}
