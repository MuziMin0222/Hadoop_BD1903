package com.JobControl;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.JobControl
 * @filename:CombineMaxAndAvg.java
 * @create:2019.10.10.09:02:06
 * @auther:李煌民
 * @description:.作业四：连接平均温度和最高温度
 **/
public class CombineMaxAndAvg extends Configured {
    static class CombineMaxAndAvgReduce extends Reducer<Text,Text,Text, NullWritable> {
        private Text k3 = new Text();
        private NullWritable v3 = NullWritable.get();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append(k2.toString()).append(",");
            for (Text v2 : v2s) {
                sb.append(v2.toString()).append(",");
            }
            this.k3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }
}
