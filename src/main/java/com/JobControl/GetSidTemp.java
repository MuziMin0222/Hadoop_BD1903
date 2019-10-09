package com.JobControl;

import com.MapReduce.RawWeatherDataParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @program:Hadoop_BD1903
 * @package:com.JobControl
 * @filename:GetSidTemp.java
 * @create:2019.10.09.14:48:28
 * @auther:李煌民
 * @description:.获得气象站和温度数据
 **/
public class GetSidTemp extends Configured{
    static class GetSidTempMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        private RawWeatherDataParser parser = new RawWeatherDataParser();
        private Text k2 = new Text();
        private DoubleWritable v2 = new DoubleWritable();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            if (this.parser.parse(v1)){
                this.k2.set(this.parser.getSid());
                this.v2.set(this.parser.getTemp());
                context.write(this.k2,this.v2);
            }
        }
    }
}
