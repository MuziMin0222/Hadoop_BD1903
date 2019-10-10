package com.JobControl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @program:Hadoop_BD1903
 * @package:com.JobControl
 * @filename:CalcAvgTemp.java
 * @create:2019.10.10.08:55:13
 * @auther:李煌民
 * @description:.作业三：计算每一个气象站的平均温度
 **/
public class CalcAvgTemp extends Configured {
    static class CalcAvgTempReduce extends Reducer<Text, Text,Text, DoubleWritable>{
        private Text k3 = new Text();
        private DoubleWritable v3 = new DoubleWritable();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            for (Text v2 : v2s) {
                count++;
                double v = Double.parseDouble(v2.toString());
                sum += v;
            }
            double avg = sum/count;
            this.k3.set(k2);
            this.v3.set(avg);
            context.write(this.k3,this.v3);
        }
    }
}
